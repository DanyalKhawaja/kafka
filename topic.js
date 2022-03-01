const { Kafka } = require("kafkajs");

const topic = process.argv[2];
const clientId = `Topic ${topic} creater`;
const brokers = ["127.0.0.1:29092", "127.0.0.1:39092", "127.0.0.1:49092"];
const topics = [
  {
    topic,
    numPartitions: 1,
    replicaAssignment: [{ partition: 0, replicas: [0, 1, 2, 3] }],
  },
];

async function createPartition() {
  const kafka = new Kafka({ clientId, brokers });
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics });
  console.log(`${topic} created`);
  await admin.disconnect();
}

createPartition();
