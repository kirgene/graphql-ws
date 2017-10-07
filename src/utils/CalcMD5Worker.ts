import SparkMD5 from 'spark-md5';

function calcMD5(f) {
  let blobSlice = Blob.prototype.slice;
  let chunkSize = 2097152;
  let chunks = Math.ceil(f.size / chunkSize);
  let spark = new SparkMD5.ArrayBuffer();
  let currentChunk = 0;

  let fr = new FileReader();
  fr.onload = function(e) {
    spark.append(e.target.result);
    if (currentChunk === chunks) {
      postMessage({name: f.name, md5: spark.end()});
    } else {
      postMessage({name: f.name, md5: currentChunk + ' / ' + chunks});
      let start = currentChunk * chunkSize;
      let end = ((start + chunkSize) >= f.size) ? f.size : start + chunkSize;
      fr.readAsArrayBuffer(blobSlice.call(f, start, end));
      currentChunk++;
    }
  };
  fr.onerror = function(e) {
    postMessage({name: f.name, md5: e.message});
  };
  // kick off the reading of the file
  fr.readAsArrayBuffer(blobSlice.call(f, 0, chunkSize));
}

onmessage = function(e) {
  e.data.forEach(function(f){
    calcMD5(f);
  });
};
