import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';
dotenv.config();

const kafka = new Kafka({ 
  clientId: 'weather-producer', 
  brokers: ['localhost:9092'] 
});

const producer = kafka.producer();

const lat = -7.557091;
const lon = 110.843809;
const api_key = process.env.OPENWEATHER_API_KEY;

let isRunning = false;
let loopTimeout = null;

if (!api_key) {
  console.error("ERROR: Missing OPENWEATHER_API_KEY in .env");
  process.exit(1);
}

const apiUrl = `https://api.openweathermap.org/data/2.5/weather?lat=${lat}&lon=${lon}&appid=${api_key}&units=metric`;

function flattenWeatherData(data) {
  return {
    timestamp: new Date().toISOString(),
    city_id: data.id,
    city_name: data.name,
    latitude: data.coord.lat,
    longitude: data.coord.lon,
    temp: data.main.temp,
    weather_main: data.weather[0].main,
    weather_desc: data.weather[0].description
  };
}

async function fetchAndProduce() {
  if (!isRunning) return;

  try {
    const response = await fetch(apiUrl);
    if (!response.ok) throw new Error(`HTTP error! Status: ${response.status}`);
    
    const rawData = await response.json();
    const flatRow = flattenWeatherData(rawData);

    // console.table(flatRow); 

    await producer.send({
      topic: 'weather-api-topic',
      messages: [ { value: JSON.stringify(flatRow) } ],
    });
    
  } catch (error) {
    console.error('Error:', error.message);
  } finally {
    if (isRunning) {
        loopTimeout = setTimeout(fetchAndProduce, 100000);
    }
  }
}

producer.on(producer.events.CONNECT, () => {
  isRunning = true;
  fetchAndProduce();
});

producer.on(producer.events.DISCONNECT, () => {
  console.error('Kafka Disconnected -> Stopping Loop');
  isRunning = false;
  if (loopTimeout) clearTimeout(loopTimeout);
});

async function startService() {
  try {
    await producer.connect();
  } catch (error) {
    console.error('Failed to connect to Kafka:', error);
    process.exit(1);
  }
}

startService();
