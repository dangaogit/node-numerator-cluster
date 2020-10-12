import { mainLog } from "./log";
import { Numerator, NumeratorOption, PushNumerator } from "./numerator";

export interface NumeratorClusterOption<T> {
  /** 以毫秒记的任务检测定时器间隔 */
  taskSeekInterval: number;
  /** 以毫秒记的睡眠检测定时器间隔 */
  sleepStudiesInterval: number;
  /** 负荷容量 */
  loadSize: number;
  /** 分子任务获取机器 */
  numeratorClusterFactory(): Promise<NumeratorOption<T>>;
  /** 更新分子信息 */
  pushNumerator: PushNumerator<T>;
  performListener(particle: number, context: T): Promise<boolean>;
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

  constructor(option: Pick<NumeratorClusterOption<T>, "numeratorClusterFactory" | "pushNumerator" | "performListener">) {
    log.info("numerator cluster init...");
    this.option = {
      sleepStudiesInterval: 1000 * 60, // 1min
      taskSeekInterval: 10, // 10ms
      loadSize: 100,
      ...option,
    };
  }

  public run() {
    this._state = "running";
    this._timeout = setInterval(this.interval.bind(this), this.option.sleepStudiesInterval);

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

  public onAcquiredNumerator() {}

  private interval() {
    new Numerator(this);
  }
}

export default NumeratorCluster;
