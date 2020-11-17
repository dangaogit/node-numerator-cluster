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
  private token = getToken();
  private option!: NumeratorOption<T>;
  private isLastBatch = false;

  constructor(private cluster: NumeratorCluster<T>) {
    this.start();
  }

  private async start() {
    const config = await this.getNumeratorConfig();

    if (!config) {
      return;
    }

    this.initConfig(config);

    const l = log.getDeriveLog(this.option.key + "");

    l.info("Running task...");

    if (!(await this.lock())) {
      return l.warn("Lock failed!");
    }

    if (this.option.state === NumeratorStateEnum.waiting) {
      await this.runTask();
      if (!(await this.unlock())) {
        return l.warn("Unlock failed!");
      }
    } else {
      if (this.option.state !== NumeratorStateEnum.running) {
        return l.warn("This state not eq running!");
      }

      if (!this.getLoadSpace()) {
        return l.warn("There is not enough space to run this task!");
      }

      if (!(await this.updateProgress())) {
        return l.warn("Update progress failed!");
      }

      if (this.isLastBatch) {
        const execResult = await this.exec();
        if (execResult.length > 0) {
          await this.pushFailedParticle(execResult);
        }

        await this.complete();

        if (!(await this.unlock())) {
          return l.warn("Unlock failed!");
        }
      } else {
        if (!(await this.unlock())) {
          return l.warn("Unlock failed!");
        }

        const execResult = await this.exec();

        if (execResult.length > 0) {
          this.pushFailedParticle(execResult);
        }

        this.complete();
      }
    }
  }

  private async getNumeratorConfig() {
    return this.cluster.option.producer();
  }

  private initConfig(config: ProducerOptionType<T>) {
    this.option = { ...config, lastRunTime: new Date(), timer: config.timer || 0, failQueue: config.failQueue || [] };
  }

  private async complete() {
    this.revertLoadSpace();
    await this.done();
  }

  private async exec() {
    const { particlePerReadCount, fulfillCount: newFulfillCount, context } = this.option;
    const fulfillCount = newFulfillCount - particlePerReadCount;
    const { option } = this.cluster;

    const promises = [];
    let results: boolean[] = [];
    if (option.consumMode === "single") {
      for (let i = fulfillCount; i < newFulfillCount; i++) {
        promises.push(option.consumer(i, context));
      }
      results = (await Promise.allSettled(promises)).map((v) => (v.status === "fulfilled" ? v.value : false));
    } else {
      const pool: number[] = [];
      for (let i = fulfillCount; i < newFulfillCount; i++) {
        pool.push(i);
      }
      results = await option.consumer(pool, context);
    }

    return results
      .map((result, index) => ({ result, index }))
      .filter((item) => !item.result)
      .map((item) => item.index);
  }

  private async pushFailedParticle(list: number[]) {
    const { key, context } = this.option;
    const option: PushStateOption<T> = { key };
    const { pushState } = this.cluster.option;

    option.failQueue = option.failQueue || [];
    option.failQueue.push(...list);
    const result = await pushState(option, context);
    if (result) {
      Object.assign(this.option, option);
    }

    return result;
  }

  private async updateProgress() {
    const { key, fulfillCount, particleCount, particlePerReadCount, context } = this.option;
    const option: PushStateOption<T> = { key };
    const { pushState } = this.cluster.option;
    const progress = fulfillCount + particlePerReadCount;
    if (progress > particleCount) {
      option.fulfillCount = particleCount;
    } else {
      option.fulfillCount = progress;
    }

    if (option.fulfillCount === particleCount) {
      this.isLastBatch = true;
    }

    const result = await pushState(option, context);
    if (result) {
      Object.assign(this.option, option);
    }

    return result;
  }

  private async runTask() {
    const { key, timer, lastRunTime, context } = this.option;
    const option: PushStateOption<T> = { key, fulfillCount: 0, failQueue: [] };
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

    const option: PushStateOption<T> = { key, locked: true, lockToken: this.token };
    const { pushState } = this.cluster.option;
    const result = await pushState(option, context);
    if (result) {
      Object.assign(this.option, option);
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
    }

    return result;
  }

  private async done() {
    const { key, fulfillCount, particleCount, timer, context } = this.option;
    const option: PushStateOption<T> = { key };
    const { pushState } = this.cluster.option;

    if (fulfillCount >= particleCount) {
      option.lastRunTime = new Date();
      option.state = timer > 0 ? NumeratorStateEnum.waiting : NumeratorStateEnum.fulfilled;
      const result = await pushState(option, context);
      if (result) {
        Object.assign(this.option, option);
      }

      return result;
    }
  }

  private getLoadSpace() {
    const { load } = this.option;
    const { loadSize } = this.cluster.option;
    const result = loadSize - load >= 0;
    if (result) {
      this.cluster.option.loadSize -= load;
    }
    return loadSize - load >= 0;
  }

  private revertLoadSpace() {
    const { load } = this.option;
    this.cluster.option.loadSize += load;
  }
}
