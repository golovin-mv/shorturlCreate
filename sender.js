const { parentPort, workerData } = require('worker_threads');

const axios = require('axios');

const HOST = 'host';
const USER = 'user';
const PASSWORD = 'password';

const makeRequest = url => axios.get(`http://${HOST}/yourls-api.php/yourls-api.php`, {
  params: {
    username: USER,
    password: PASSWORD,
    action: "shorturl",
    format: "json",
    url: url
  }
});

const createShortLink = async (id, url) => {
  try {
    const r = await makeRequest(url);
    parentPort.postMessage({
      id,
      url,
      shortUrl: r.data.shorturl
    });

  } catch (err) {

    parentPort.postMessage({
      id,
      url,
      shortUrl: err
    });
  }
}


const process = async (data) => {
  for (el of data) {
    await createShortLink(el['CONTRACT_ID'], el.URL);
  }
}

process(workerData)
  .catch(err => console.log(err));
