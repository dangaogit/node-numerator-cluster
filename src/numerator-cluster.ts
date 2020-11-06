import { mainLog } from "./log";
import { Numerator, NumeratorOption } from "./numerator";

// export type PartialNotRequired<T, K extends keyof T, O extends keyof T = Exclude<keyof T, K>> = {
//   [P in K]?: T[P];
// } &
//   Pick<T, O>;

export type PartialNotRequired<T, K extends keyof T> = Partial<T> & Omit<T, K>;
export type PartialRequired<T, K extends keyof T> = Partial<T> & Pick<T, K>;

export type ProducerOptionType<T> = PartialNotRequired<NumeratorOption<T>, "timer" | "failQueue" | "lastRunTime">;

export interface NumeratorClusterOption<T> {
  /** 以毫秒记的任务检测定时器间隔 */
  taskSeekInterval: number;
  /** 负荷容量 */
  loadSize: number;
  /** 分子任务获取 */
  producer(): Promise<ProducerOptionType<T> | void>;
  /** 更新分子信息 */
  pushState(option: PartialRequired<NumeratorOption<T>, "key">): Promise<boolean>;
  /** 分子消费 */
  consumer(particle: number, context: T): Promise<boolean>;
}

export type NumeratorClusterStateType = "init" | "running" | "pause" | "stop";

const log = mainLog.getDeriveLog("NumeratorCluster");

export class NumeratorCluster<T> {
  private _state: NumeratorClusterStateType = "init";
  private _timeout?: NodeJS.Timeout;

  public get state() {
    return this._state;
  }

  public set state(value) {
    throw "Please don't set state!";
  }

  public option!: NumeratorClusterOption<T>;

  constructor(option: PartialRequired<NumeratorClusterOption<T>, "producer" | "consumer" | "pushState">) {
    log.info("numerator cluster init...");
    this.option = {
      taskSeekInterval: 1000, // 1s
      loadSize: 100,
      ...option,
    };
  }

  public run() {
    this._state = "running";
    new Numerator(this);
    this._timeout = setInterval(() => {
      new Numerator(this);
    }, this.option.taskSeekInterval);

    log.info("Set state running");

    return this;
  }

  public pause() {
    this._state = "pause";
    if (this._timeout) {
      clearInterval(this._timeout);
    }
    log.info("Set state pause");
    return this;
  }

  public stop() {
    this._state = "stop";
    if (this._timeout) {
      clearInterval(this._timeout);
    }
    log.info("Set state stop");
    return this;
  }
}

export default NumeratorCluster;
