const WebSocket = require("ws");
const { prisma } = require("../services/prismaClient/prismaClient");
const { getWaitingJobs } = require("../processors/StockProcessor/WaitingQueue");
const redisConnection = require("../processors/redis-connection");
const { Queue } = require("bullmq");
const { Client } = require("pg");
const dayjs = require("dayjs");

const ETFQueueWebSocket = new Queue("ETFQueueWebSocket", {
  connection: redisConnection,
});

const pgClient = new Client({
  connectionString: process.env.DATABASE_URL,
});

pgClient.connect();

async function etfSocket() {
  
  const queryResult = await pgClient.query(
    `SELECT * FROM us."Etf" WHERE "isIpos" = false limit 5000`
  );

  const allTickers = queryResult.rows.map((row) => row.ticker);
  
  const url =
    "wss://ws.eodhistoricaldata.com/ws/us?api_token=";
  const socket = new WebSocket(url);

  async function processTickersBatch(allTickers) {
    const batchSize = 5; // Define your batch size as needed
    const numBatches = Math.ceil(allTickers.length / batchSize);

    for (let i = 0; i < numBatches; i++) {
      const batchTickers = allTickers.slice(i * batchSize, (i + 1) * batchSize);
      console.log("IN Batch Processing functions", batchTickers);
      await subscribeToTickers(batchTickers);
    }
  }

  async function subscribeToTickers(tickers) {
    try {
      let timeouts = {};
      let receivedDataForTickers = new Set();

      socket.on("open", () => {
        console.log("Connected to the WebSocket server");
        tickers.forEach((ticker) => {
          console.log("ETFWS - Subscribing to ticker:", ticker);
          const subscribeMessage = JSON.stringify({
            action: "subscribe",
            symbols: ticker,
          });

          console.log("ETF TICKER WS NEW:", subscribeMessage);
          socket.send(subscribeMessage);

          // Set a 5-minute timeout for each ticker
          timeouts[ticker] = setTimeout(() => {
            console.log(`No data received for ticker ${ticker} in 15 minutes.`);
            receivedDataForTickers?.add(ticker);
            checkAndCloseSocket();
          }, 900000);
        });
      });

      let tickerIds = [];
      console.log("SOCKET START 2");

      socket.on("message", async (message) => {
        try {
          const data = JSON.parse(message);
          if (data.s && timeouts[data.s]) {
            const exists = tickerIds.some((item) => item?.s === data?.s);

            if (!exists) {
              try {
                tickerIds?.push(data);
                const waiting = await getWaitingJobs(
                  "ETFQueueWebSocket",
                  "repopulateETFsWS"
                );
                const hasWaiting = waiting?.some(
                  (item) => item?.data?.s === data?.s
                );
                const isPaused = await ETFQueueWebSocket?.isPaused();

                if (!hasWaiting && !isPaused) {
                  console.log("ETFWS Object added to the array.", data?.s);

                  await ETFQueueWebSocket?.add("repopulateETFsWS", data, {
                    timeout: 30000,
                  });
                }
              } catch (e) {
                console.log(
                  "********************this is an error in WSSocketQueue ************",
                  e
                );
              }
            }

            clearTimeout(timeouts[data.s]);
            delete timeouts[data.s];
            receivedDataForTickers?.add(data.s);
            checkAndCloseSocket();
          }
        } catch (error) {
          console.error("Error parsing message:", error);
        }
      });

      const checkAndCloseSocket = () => {
        if (receivedDataForTickers.size === allTickers.length) {
          socket.close();
        }
      };

      socket.on("close", (code, reason) => {
        console.log(`Connection closed (code: ${code}, reason: ${reason})`);
      });

      socket.on("error", (error) => {
        console.error("WebSocket error:", error);
      });
    } catch (e) {
      console.log("ERRR", e);
    }
  }

  processTickersBatch(allTickers)
    .then(() => console.log("All tickers processed successfully"))
    .catch((error) => console.error("Error processing tickers:", error));
}

module.exports = etfSocket;
