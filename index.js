const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const csvtojson = require("csvtojson");
const ProgressBar = require('progress');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

/**
 * Количество потоков
 * чем больше тем быстрее но вероятность
 * ошибок и все положить выше
 */
const THREAD_COUNT = 2;

/**
 * Путь в исходному реестру в формате csv
 * CONTRACT_ID  |URL
 * 3 004 379 512|http://vostbank.ru/...
 */
const filePath = './t.csv';
/**
 * Путь к отчету
 */
const resultFilePath = './res.csv';

const csvWriter = createCsvWriter({
  path: resultFilePath,
  header: [
    { id: 'id', title: 'CONTRACT_ID' },
    { id: 'url', title: 'URL' },
    { id: 'shortUrl', title: 'SHORT_URL' },
  ],
  append: true,
  fieldDelimiter: ';',
  alwaysQuote: true
})

/**
 * @param {String} filePath 
 */
const readFile = filePath => csvtojson({
  delimiter: ';'
})
  .fromFile(filePath)


const writeResult = (error) => {
  return csvWriter.writeRecords([{
    id: error.id,
    url: error.url,
    shortUrl: error.shortUrl
  }])
    .catch(err => `Write file error: ${err}`)
}

const chunk = (array, size) => {
  const chunked_arr = [];
  let index = 0;
  while (index < array.length) {
    chunked_arr.push(array.slice(index, size + index));
    index += size;
  }
  return chunked_arr;
}

const createWorker = (data, onMessage, onError, onExit) => {
  let worker = new Worker('./sender.js', {workerData: data});

  worker.on('message', onMessage)
  worker.on('error', onError);
  worker.on('exit', onExit);
  
  return worker;
}

const run = data => {
  const bar = new ProgressBar(':bar :current/:total (:percent) :elapsed s', { total: data.length });
  // получаем заявки для воркеров
  chunk(data, Math.floor(data.length/THREAD_COUNT))
    .map((el, i) => createWorker(
      el,
      mes => writeResult(mes) && bar.tick(),
      err => console.log(err),
      () => console.log(i + ' worker exit')
    ));
};

readFile(filePath)
  .then(run)