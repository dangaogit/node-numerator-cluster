import { NumeratorCluster, NumeratorStateEnum } from "../src";

jest.useFakeTimers();

describe("Test numrator-cluster", () => {
  test("Test taskSeekInterval", async () => {
    const clus = new NumeratorCluster({
      async producer() {
        return {
          context: {},
          fulfillCount: 0,
          key: 1,
          load: 10,
          lockToken: "",
          locked: false,
          particleCount: 100,
          particlePerReadCount: 10,
          state: NumeratorStateEnum.waiting,
        };
      },
      async consumer(particle, context) {
        console.log(particle, context);
        return true;
      },
      async pushState(option) {
        console.log(option);
        return true;
      },
    });

    clus.run();
  });
});
