import { NumeratorCluster, NumeratorStateEnum } from "../src";

const configs = [
  { context: {}, fulfillCount: 0, key: 1, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
  { context: {}, fulfillCount: 0, key: 2, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
  { context: {}, fulfillCount: 0, key: 3, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
  { context: {}, fulfillCount: 0, key: 4, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
];

const clus = new NumeratorCluster({
  async producer() {
    console.log("生产配置");
    return configs.find((v) => v.state === NumeratorStateEnum.waiting || v.state === NumeratorStateEnum.running);
  },
  async consumer(particle, context) {
    console.log(`消费${particle}`);
    return true;
  },
  async pushState(option) {
    console.log(`更新状态`);
    const config = configs.find((v) => v.key === option.key);
    Object.assign(config, option);
    console.log(option);
    return true;
  },
});

clus.run();
