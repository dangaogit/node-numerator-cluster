import { NumeratorCluster, NumeratorStateEnum } from "../src";
import cluster from "cluster";
import os, { cpus } from "os";
import http from "http";
import { resolve } from "path";
import { config } from "process";

const configs = [
  { context: {}, fulfillCount: 0, key: 1, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
  // { context: {}, fulfillCount: 0, key: 2, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
  // { context: {}, fulfillCount: 0, key: 3, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
  // { context: {}, fulfillCount: 0, key: 4, load: 10, lockToken: "", locked: false, particleCount: 100, particlePerReadCount: 10, state: NumeratorStateEnum.waiting },
];

let count = 0;
let pushStateCount = 0;

function clusterTest() {
  if (cluster.isMaster) {
    const cpus = os.cpus();
    console.log("This master, cpu length", cpus.length);

    const server = http.createServer((req, res) => {
      if (req.method!.toUpperCase() === "GET") {
        if (configs[0].state === NumeratorStateEnum.running || configs[0].state === NumeratorStateEnum.waiting) {
          res.write(JSON.stringify(configs[0]));
        }
        res.end();
      } else if (req.method!.toUpperCase() === "PUT") {
        let data: Buffer;
        req.on("data", (chunk) => {
          data = data ? Buffer.concat([data, chunk]) : chunk;
        });
        req.on("end", () => {
          Object.assign(configs[0], JSON.parse(data.toString()));
          res.write(JSON.stringify({ code: 200, msg: "ok" }));
          res.end();
        });
      }
    });

    server.listen(8999).on("listening", () => {
      console.log("start server ok!");
    });

    for (let i = 0; i < cpus.length; i++) {
      cluster.fork();
    }

    cluster.on("exit", () => {
      console.log("master exit");
    });
  } else {
    console.log("进程id", process.pid);
    const clus = new NumeratorCluster({
      async producer() {
        // console.log("生产配置", count, pushStateCount);
        // const config = configs.find((v) => v.state === NumeratorStateEnum.waiting || v.state === NumeratorStateEnum.running);
        // console.log(JSON.stringify(config));

        return new Promise((resolve) => {
          let data = "";
          http.get("http://127.0.0.1:8999", (res) => {
            res.on("data", (chunk) => (data += chunk));
            res.on("end", () => {
              console.log("拉取配置", data);
              try {
                const json = JSON.parse(data);
                console.log(json);
                resolve(json);
              } catch (error) {
                resolve();
              }
            });
          });
        });
      },
      consumMode: "single",
      async consumer(particle, context) {
        // console.log(`消费${particle}`);
        count++;
        // await sleep(1000);
        return true;
      },
      async pushState(option) {
        // console.log(`更新状态`);
        // pushStateCount++;

        // console.log(option)

        return new Promise((resolve) => {
          const req = http.request({ method: "put", host: "127.0.0.1", port: 8999 }, (res) => {
            if (res.statusCode === 200) {
              resolve(true);
            }
          });
          req.write(JSON.stringify(option));
          req.end();
        });
      },
    });

    clus.run();
  }
}

function signleTest() {
  const clus = new NumeratorCluster({
    async producer() {
      // console.log("生产配置", count, pushStateCount);
      const config = configs.find((v) => v.state === NumeratorStateEnum.waiting || v.state === NumeratorStateEnum.running);
      // console.log(JSON.stringify(config));
      // console.log("生产", config!.fulfillCount);
      return config;
    },
    async consumer(particle, context) {
      console.log(`消费${particle}`);
      count++;
      await sleep(3000);
      return [];
    },
    consumMode: "multiple",
    async pushState(option) {
      console.log("pushState", JSON.stringify(option));
      // console.log(`更新状态`, option.fulfillCount);
      // pushStateCount++;
      const config = configs.find((v) => v.key === option.key)!;
      // if(config.locked && config.lockToken !== option.lockToken) {
      //   return false;
      // }
      Object.assign(config, option);
      // console.log(option);
      return true;
    },
  });

  async function sleep(timer: number) {
    return new Promise((resolve) => setTimeout(resolve, timer));
  }

  clus.run();
}
signleTest();

// clusterTest();
