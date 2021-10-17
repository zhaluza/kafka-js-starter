const { Kafka } = require("kafkajs");

run().then(
  () => console.log("Done"),
  (err) => console.error(err)
);

async function run() {
  const kafka = new Kafka({ brokers: ["localhost:9092"] });
  /**
   * If you specify the same group id and run this process multiple times, KafkaJS
   * won't get the events. That's because Kafka assumes that, if you specify a
   * group id, a consumer in that group id should only read each message at most once.
   */

  const producer = kafka.producer();
  await producer.connect();

  // Wait 1 second before sending a new message
  await new Promise((resolve) => setTimeout(resolve, 1000));

  const randomNum = Math.floor(Math.random() * 1000);

  await producer.send({
    topic: "quickstart-events",
    messages: [{ value: `event ${randomNum}` }],
  });
}
