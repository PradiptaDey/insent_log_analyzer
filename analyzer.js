const AWS = require("aws-sdk");
const s3 = new AWS.S3({apiVersion: '2006-03-01'});
const zlib = require('zlib');
const fs = require('fs');
const readline = require('readline');
const moment = require('moment');

const params = {
  Bucket: 'insent-hiring-assignment-data-samples/log-analyzer/1938c1b5-22eb-47b9-a079-26395210631b/i-05e5e20f4af2d3987',
  Key: "00000.gz"
};

//create read stream from the raw data returned from s3 bucket
const s3Stream = s3.getObject(params).createReadStream();
const unzip = zlib.createGunzip();

// Listen for errors returned by the service
s3Stream.on('error', function(err) {
    // NoSuchKey: The specified key does not exist
    console.error(err);
});

//extract the .gz file through stream
s3Stream.pipe(unzip);
//to read data line by line
const readInterface = readline.createInterface({
  input: unzip
});

const suspiciousList = new Set();

try {
  fs.rmdirSync('data', { recursive: true });
  fs.rmdirSync('average-data', { recursive: true });
} catch (e) {
  console.error(`Error while deleting.`);
}
try {
  fs.mkdirSync('data');
} catch (e) {
  console.error(`Error occured while creating directory.`);
}
const lineArr = [];
//process the data on each line received
readInterface.on('line', function(line) {
  lineArr.push(line);
  readInterface.pause();
  _processData(lineArr.shift());
});

function _processData(line) {
  //derectly process the data instead of writing into a separate file to reduce execution time
  if (line.indexOf('/user/pageVisit/spentTime') !== -1) {
    const time = line.match(/^[0-9].*?Z/)[0];
    const ids = line.match(/spentTime\/([a-z0-9]*)\/([a-z0-9]*)\b/);
    const sessionId = ids[1];
    const pageId = ids[2];
    let pageDetails = {};
    pageDetails[pageId] = {};
    pageDetails[pageId][sessionId] = { 'timeList': [time] };
    const path = `data/${pageId}.txt`;
    if (fs.existsSync(path)) {
      const fileData = fs.readFileSync(path, 'utf8');
      try {
        pageDetails = JSON.parse(fileData);
      } catch (err) {
        console.log("error occureed while parsing");
      }
    }
    //if the combination of session and page already present in the suspicious list dont make memory overload
    if (pageDetails[pageId]) {
      const sessionBasedLog = pageDetails[pageId];
      if (sessionBasedLog[sessionId]) {
        const visitedAtList = sessionBasedLog[sessionId]['timeList'];
        let isSuspicious = false;
        //to detect the suspicious session
        if (!sessionBasedLog[sessionId].isSuspicious) {
          isSuspicious = visitedAtList.find((el, index) => {
            const prevTime = moment(el);
            const next = moment(time);
            //if the difference of time is more than 10 seconds return true
            return visitedAtList.length - index >= 10 && next.diff(prevTime, 'seconds') < 10
          });
        }
        if (isSuspicious) {
          sessionBasedLog['isSuspicious'] = true;
          suspiciousList.add(`${sessionId}-${pageId}`);
        }
        visitedAtList.push(time);
        sessionBasedLog[sessionId] = { 'timeList': visitedAtList };
      } else {
        sessionBasedLog[sessionId] = { 'timeList': [time] };
      }
      pageDetails[pageId] = sessionBasedLog;
    }

    fs.writeFileSync(path, JSON.stringify(pageDetails));
    if (lineArr.length > 0) {
      _processData(lineArr.shift());
    } else {
      readInterface.resume();
    }
  }
}

//TODO: we can write a file using the suspicious ids
readInterface.on('close', function() {
  console.log("suspiciousList:", suspiciousList);
  // _findAverageNumberOfSeconds();
});


//find the frequency by page and session
function _findAverageNumberOfSeconds() {
  if (fs.existsSync('data')) {
    const files = fs.readdirSync('data');
    if (files.length > 0) {
      files.forEach(file => {
        const averageData = {};
        const fileData = fs.readFileSync(`data/${file}`, 'utf8');
        const pageDetails = JSON.parse(fileData);
        const pageId = Object.keys(pageDetails)[0];
        averageData[pageId] = {};
        console.log(Object.keys(Object.values(pageDetails)[0]));
        Object.keys(Object.values(pageDetails)[0]).forEach(el => {
          const sessionId = el;
          const timeDiffArr = [];
          const times = Object.values(pageDetails)[0][el];
          for (let i = 0; i < times.length - 2; i++) {
            timeDiffArr.push(moment(times[i+1]).diff(moment(times[i]), 'seconds'));
          }
          const sum = timeDiffArr.reduce(function(a, b){
            return a + b;
          }, 0);
          //storing in milli seconds
          averageData[pageId][sessionId] = Math.floor((sum/timeDiffArr.length)*1000);
        });
        if (!fs.existsSync('average-data')) {
          fs.mkdirSync('average-data');
        }
        fs.writeFileSync(`average-data/${pageId}.txt`, JSON.stringify(averageData));
      });
    }
  }
}