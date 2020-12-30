import { Log } from "@dangao/node-log";
import { createHash } from "crypto";
import { mainLog } from "./log";
import NumeratorCluster, { PartialRequired, ProducerOptionType } from "./numerator-cluster";
export enum NumeratorStateEnum {
  init,
  running,
  pause,
  fulfilled,
  rejected,
  waiting,
}

export type PushStateOption<T = string> = PartialRequired<NumeratorOption<T>, "key">;

export interface NumeratorOption<T = string> {
  /** 主码 */
  key: string | number;
  /** 总粒子数 */
  particleCount: number;
  /** 每次读取粒子阈值 */
  particlePerReadCount: number;
  /** 已分配粒子数 */
  allocatedCount: number;
  /** 已完成粒子数 */
  fulfillCount: number;
  /** 失败粒子队列 */
  failQueue: number[];
  locked: boolean;
  lockToken: string;
  load: number;
  context: T;
  timer: number;
  lastRunTime: Date;
  state: NumeratorStateEnum;
}

const log = mainLog.getDeriveLog("Numerator");

function getToken() {
  const hash = createHash("sha1");
  const time = new Date().getTime().toString();
  const [r1, r2] = [Math.random().toString(), Math.random().toString()];
  hash.write(`${time}-${r1}-${r2}`);
  return hash.digest().toString("hex");
}

export class Numerator<T> {
  public static log = mainLog;

  private token?: string;
  private option!: NumeratorOption<T>;
  private log!: Log;

  constructor(private cluster: NumeratorCluster<T>) {
    this.start();
  }

  private async start() {
    const config = await this.getNumeratorConfig();

    if (!config) {
      return;
    }

    this.initConfig(config);
    this.log = log.getDeriveLog(this.option.key + "");

    const { log: l } = this;
    const { particleCount, fulfillCount, allocatedCount, state } = this.option;

    if (allocatedCount > particleCount) {
      // 该任务已经分配完，只是还未执行完，不做任何操作
      return;
    }

    try {
      l.info("Running task...");
      await this.lock();

      if (state === NumeratorStateEnum.waiting) {
        /** 启动任务并更新状态 */
        await this.setStateRunning();
        await this.cluster.option.onSetState(this.option.state, state, this.option);
      } else if (state === NumeratorStateEnum.running) {
        if (fulfillCount >= particleCount) {
          await this.taskDone();
          await this.cluster.option.onSetState(this.option.state, state, this.option);
        } else if (this.getLoadSpace()) {
          this.exec();
          await this.updateProgress();
        }
      }

      await this.unlock();
    } catch (error) {
      l.error(error);
      await this.unlock();
    }
  }

  private async getNumeratorConfig() {
    return this.cluster.option.producer();
  }

  private initConfig(config: ProducerOptionType<T>) {
    this.option = { ...config, lastRunTime: new Date(), timer: config.timer || 0, failQueue: config.failQueue || [] };
  }

  private async exec() {
    const { particlePerReadCount, allocatedCount, context } = this.option;
    const { option } = this.cluster;

    const promises = [];
    let results: boolean[] = [];
    if (option.consumMode === "single") {
      for (let i = allocatedCount; i < allocatedCount + particlePerReadCount; i++) {
        promises.push(option.consumer(i, context, this.option));
      }
      results = (await Promise.allSettled(promises)).map((v) => (v.status === "fulfilled" ? v.value : false));
    } else {
      const pool: number[] = [];
      for (let i = allocatedCount; i < allocatedCount + particlePerReadCount; i++) {
        pool.push(i);
      }
      results = await option.consumer(pool, context, this.option);
    }

    this.revertLoadSpace();

    await this.updateFulfillProgress();

    return results
      .map((result, index) => ({ result, index }))
      .filter((item) => !item.result)
      .map((item) => item.index);
  }

  /** 更新完成进度 */
  private async updateFulfillProgress() {
    let result;
    await this.lock();

    try {
      const { pushState, queryConfig } = this.cluster.option;
      const { key, context, particlePerReadCount, particleCount } = this.option;
      const config = await queryConfig(key);

      if (!config) {
        throw `Cannot find config '${key}'`;
      }

      const { fulfillCount } = config;
      const option: PushStateOption<T> = { key };
      const progress = fulfillCount + particlePerReadCount;

      if (progress > particleCount) {
        option.fulfillCount = particleCount;
      } else {
        option.fulfillCount = progress;
      }

      result = await pushState(option, context);

      if (result) {
        Object.assign(this.option, option);
      } else {
        throw "Update fulfill-progress failed!";
      }
    } catch (error) {
      this.log.error(error);
    }

    await this.unlock();
    return result;
  }

  /** 更新分配进度 */
  private async updateProgress() {
    const { key, allocatedCount, particleCount, particlePerReadCount, context } = this.option;
    const option: PushStateOption<T> = { key };
    const { pushState } = this.cluster.option;
    const progress = allocatedCount + particlePerReadCount;

    if (progress > particleCount) {
      option.allocatedCount = particleCount;
    } else {
      option.allocatedCount = progress;
    }

    const result = await pushState(option, context);
    if (result) {
      Object.assign(this.option, option);
    } else {
      throw "Update progress failed!";
    }

    return result;
  }

  private async setStateRunning() {
    const { key, timer, lastRunTime, context } = this.option;
    const option: PushStateOption<T> = { key, fulfillCount: 0, allocatedCount: 0, failQueue: [] };
    const { pushState } = this.cluster.option;

    /**
     * timer小于0代表只执行一次
     */
    if (timer <= 0 || Date.now() - lastRunTime.getTime() >= timer) {
      option.state = NumeratorStateEnum.running;
      const result = await pushState(option, context);
      if (result) {
        Object.assign(this.option, option);
      }

      return result;
    }
  }

  private async lock() {
    const { locked, key, context } = this.option;
    if (locked) {
      return false;
    }

    this.token = getToken();

    const option: PushStateOption<T> = { key, locked: true, lockToken: this.token };
    const { pushState } = this.cluster.option;
    const result = await pushState(option, context);
    if (result) {
      Object.assign(this.option, option);
    } else {
      throw "Lock failed!";
    }

    return result;
  }

  private async unlock() {
    const { key, locked, lockToken, context } = this.option;
    if (!locked || lockToken !== this.token) {
      return false;
    }

    const option: PushStateOption<T> = { key, locked: false };
    const { pushState } = this.cluster.option;
    const result = await pushState(option, context);
    if (result) {
      Object.assign(this.option, option);
    } else {
      throw "Unlock failed!";
    }

    return result;
  }

  private async taskDone() {
    const { key, timer, context } = this.option;
    const option: PushStateOption<T> = { key };
    const { pushState } = this.cluster.option;

    option.lastRunTime = new Date();
    option.state = timer > 0 ? NumeratorStateEnum.waiting : NumeratorStateEnum.fulfilled;

    const result = await pushState(option, context);
    if (result) {
      Object.assign(this.option, option);
    } else {
      throw "Set done failed!";
    }

    return result;
  }

  private getLoadSpace() {
    const { load } = this.option;
    const { loadSize } = this.cluster.option;
    const result = loadSize - load >= 0;
    if (result) {
      this.cluster.option.loadSize -= load;
    } else {
      this.log.warn("There is not enough space to run this task!");
    }
    return loadSize - load >= 0;
  }

  private revertLoadSpace() {
    const { load } = this.option;
    this.cluster.option.loadSize += load;
  }
}
