import { createHash } from "crypto";
import { mainLog } from "./log";
import NumeratorCluster from "./numerator-cluster";
export enum NumeratorStateEnum {
  init,
  running,
  pause,
  fulfilled,
  rejected,
  // 等待执行，一般是定时任务会有
  waiting,
}

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
  failQueue: number[] | null;
  locked: boolean;
  lockToken: string;
  load: number;
  context: T;
  timer: number;
  lastRunTime: Date;
  state: NumeratorStateEnum;
}

export interface PushNumerator<T = string> {
  (option: NumeratorOption<T>): Promise<boolean>;
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

  constructor(private cluster: NumeratorCluster<T>) {
    this.start();
  }

  private async start() {
    const config = await this.getNumeratorConfig();
    const { state, key } = config;
    const l = log.getDeriveLog(key + "");

    this.option = config;

    if (!(await this.lock())) {
      return l.warn("Lock failed!");
    }

    if (state === NumeratorStateEnum.waiting) {
      // 定时任务，判断间隔时间是否满足要求，满足时将状态更新至running
      return this.runScheduleTask();
    }

    if (state !== NumeratorStateEnum.running) {
      return l.warn("This state not eq running!");
    }

    if (!this.getLoadSpace()) {
      return l.warn("There is not enough space to run this task!");
    }

    if (!(await this.updateProgress())) {
      return l.warn("Update progress failed!");
    }

    if (!(await this.unlock())) {
      return l.warn("Unlock failed!");
    }

    const execResult = await this.exec();

    if (execResult.length > 0) {
      this.pushFailedParticle(execResult);
    }

    this.complete();
  }

  private async getNumeratorConfig() {
    return this.cluster.option.numeratorClusterFactory();
  }

  private complete() {
    this.revertLoadSpace();
    this.done();
  }

  private async exec() {
    const { particleCount, particlePerReadCount, fulfillCount, context } = this.option;
    const { performListener } = this.cluster.option;
    const failedList: number[] = [];
    const targetCount = fulfillCount + particlePerReadCount > particleCount ? particleCount : fulfillCount + particlePerReadCount > particleCount;

    for (let i = fulfillCount; i < targetCount; i++) {
      const result = await performListener(i, context);
      if (!result) {
        failedList.push(i);
      }
    }

    return failedList;
  }

  private async pushFailedParticle(list: number[]) {
    const option = { ...this.option };
    const { pushNumerator } = this.cluster.option;

    option.failQueue ||= [];
    option.failQueue.push(...list);
    return pushNumerator(option);
  }

  private async updateProgress() {
    const option = { ...this.option };
    const { pushNumerator } = this.cluster.option;
    const progress = option.fulfillCount + option.particlePerReadCount;
    if (progress > option.particleCount) {
      option.fulfillCount = option.particleCount;
    } else {
      option.fulfillCount = progress;
    }

    return pushNumerator(option);
  }

  private async runScheduleTask() {
    const option: NumeratorOption<T> = { ...this.option, fulfillCount: 0, failQueue: [] };
    const { pushNumerator } = this.cluster.option;

    if (Date.now() - option.lastRunTime.getTime() >= option.timer) {
      option.state = NumeratorStateEnum.running;
    }

    return pushNumerator(option);
  }

  private async lock() {
    if (this.option.locked) {
      return false;
    }

    const option: NumeratorOption<T> = { ...this.option, locked: true, lockToken: this.token };
    const { pushNumerator } = this.cluster.option;
    return pushNumerator(option);
  }

  private async unlock() {
    if (!this.option.locked || this.option.lockToken !== this.token) {
      return false;
    }

    const option: NumeratorOption<T> = { ...this.option, locked: false };
    const { pushNumerator } = this.cluster.option;
    return pushNumerator(option);
  }

  private async done() {
    const option: NumeratorOption<T> = { ...this.option, state: NumeratorStateEnum.fulfilled, lastRunTime: new Date() };
    const { pushNumerator } = this.cluster.option;
    return pushNumerator(option);
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
