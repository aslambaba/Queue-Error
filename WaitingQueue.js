const { Queue } = require("bullmq");
const redisConnection = require("./redis-connection");

async function getWaitingJobs(queueName, jobName) {
  const queue = new Queue(queueName, {
    connection: redisConnection
  });
  let waitingJobs = [];

  const jobs = await queue?.getJobs(["waiting"]);

  for (let job of jobs) {
    if (job?.name === jobName) {
      waitingJobs?.push({
        id: job?.id,
        state: "waiting",
        data: job?.data
      });
    }
  }

  return waitingJobs;
}

module.exports = { getWaitingJobs };
