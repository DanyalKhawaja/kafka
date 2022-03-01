const { Kafka, logLevel } = require("kafkajs");

const topic = process.argv[2];
const clientId = `${topic} Orderbook`;
const brokers = ["127.0.0.1:29092", "127.0.0.1:39092","127.0.0.1:49092"];

async function consume() {
  const kafka = new Kafka({ clientId, brokers, logLevel: logLevel.NOTHING });
  const consumer = kafka.consumer({ groupId: clientId });
  await consumer.connect();
  console.log(`${clientId} is connected as consumer`);
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ partition,message }) => {
      consumer.pause([{ topic }]);

      await delay();
      await consumer.commitOffsets([
        { topic, partition, offset: message.offset+1 },
      ]);

      consumer.resume([{ topic }]);

      console.log(`${clientId} partition ${partition} ${message.offset}  ${message.value.toString()}`);
    },
  });
}

consume();

function delay() {
  return new Promise((res) => {
    let count = 0;
    while (count < 1e5) {
      count++;
    }
    res();
  });
}
