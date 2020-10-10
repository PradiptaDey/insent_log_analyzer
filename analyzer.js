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


const structuredLogs = new Map();
const suspiciousList = [];

//process the data on each line received
readInterface.on('line', function(line) {
  //derectly process the data instead of writing into a separate file to reduce execution time
  if (line.indexOf('/user/pageVisit/spentTime') !== -1) {
    const time = line.match(/^[0-9].*?Z/)[0];
    const ids = line.match(/spentTime\/([a-z0-9]*)\/([a-z0-9]*)\b/);
    const sessionId = ids[1];
    const pageId = ids[2];
    //if the combination of session and page already present in the suspicious list dont make memory overload
    if(!suspiciousList.find(el => el.sessionId === sessionId && el.pageId === pageId)) {
      if (structuredLogs.has(sessionId)) {
        const pageDetails = structuredLogs.get(sessionId);
        if (pageDetails.has(pageId)) {
          let visitedAtList = pageDetails.get(pageId);
          //to detect the suspicious session
          const obj = _detectActivity(visitedAtList, time);
          if (obj.isSuspicious) {
            pageDetails.delete(pageId);
            suspiciousList.push({ sessionId, pageId });
          } else {
            visitedAtList = obj.timeList;
            pageDetails.set(pageId, visitedAtList);
          }
        } else {
          pageDetails.set(pageId, [time]);
        }
        structuredLogs.set(sessionId, pageDetails);
      } else {
        structuredLogs.set(sessionId, new Map().set(pageId, [time]));
      }
    }
  }
});

//TODO: we can write a file using the suspicious ids
readInterface.on('close', function() {
  console.log("-----", suspiciousList);
});

//detecting the session visting same page more than 10 times with in 10 seconds
function _detectActivity(timeList, time) {
  const indexList = [];
  let isSuspicious = false;
  timeList.forEach((el, index) => {
    const prevTime = moment(el);
    const next = moment(time);
    //if the difference of time is more than 10 seconds put the ids to an array to remove the same
    if (next.diff(prevTime, 'seconds') > 10 ) {
      indexList.push(index);
    } else if (timeList.length - index >= 10) {
      //detected suspicious
      isSuspicious = true;
    }
  });

  if (!isSuspicious) {
    //reduce memory usage
    indexList.forEach(el => {
      timeList.splice(el, 1);
    });
    //push the current time
    timeList.push(time);
  }

  return { isSuspicious, timeList };
}