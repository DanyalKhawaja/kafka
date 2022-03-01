const { Kafka } = require("kafkajs");

const dapiId = process.argv[2];
const clientId = `DAPI ${dapiId}`;
const brokers = ["127.0.0.1:29092","127.0.0.1:39092","127.0.0.1:49092"];

async function produce() {
  const kafka = new Kafka({ clientId, brokers });
  const producer = kafka.producer();
  await producer.connect();
  console.log(`${clientId} is connected`);
  let index = 0;
  let indexes = [0,0,0,0];
  setInterval(async ()=>
 {
    let symbolIndex = index%4;
    let topic = ["KSM", "MOVR", "BTC", "ETH"][symbolIndex];
    let response = await producer.send({
      topic,
      acks: 1,
      messages: [
        {
          partition: indexes[symbolIndex] % 2,
          value: `${clientId} ${indexes[symbolIndex]}`
        },
      ],
    });
    index++;
    indexes[symbolIndex]++;
    console.log(`${clientId}-${topic}-${JSON.stringify(response)}`);
  },2500);
}

produce();
