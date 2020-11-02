import { NumeratorCluster, NumeratorStateEnum } from "../src";
import cluster from "cluster";
import os, { cpus } from "os";

const configs = [
  { context: {}, fulfillCount: 0, key: 1, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
  // { context: {}, fulfillCount: 0, key: 2, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
  // { context: {}, fulfillCount: 0, key: 3, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
  // { context: {}, fulfillCount: 0, key: 4, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
];

let count = 0;
let pushStateCount = 0;

if (cluster.isMaster) {
  const cpus = os.cpus();
  console.log("This master, cpu length", cpus.length);
  for (let i = 0; i < cpus.length; i++) {
    cluster.fork();
  }

  cluster.on("exit", () => {
    console.log("master exit");
  });
} else {
  console.log("进程id", process.pid);
  
}

// const clus = new NumeratorCluster({
//   async producer() {
//     // console.log("生产配置", count, pushStateCount);
//     const config = configs.find((v) => v.state === NumeratorStateEnum.waiting || v.state === NumeratorStateEnum.running);
//     console.log(JSON.stringify(config));
//     return config;
//   },
//   async consumer(particle, context) {
//     // console.log(`消费${particle}`);
//     count++;
//     await sleep(1000);
//     return true;
//   },
//   async pushState(option) {
//     // console.log(`更新状态`);
//     pushStateCount++;
//     const config = configs.find((v) => v.key === option.key);
//     Object.assign(config, option);
//     // console.log(option);
//     return true;
//   },
// });

// async function sleep(timer: number) {
//   return new Promise((resolve) => setTimeout(resolve, timer));
// }

// clus.run();
