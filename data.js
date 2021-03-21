class Query {
  /** 
  * Class used to query NOAA Institutional Repository JSON API.
  **/

  constructor(apiURL) {
    this.apiURL = apiURL;
    this.rowTotal = fetcRowTotal(apiURL);
    this.collectionJSON = '';
    this.collectionMemberItemCount = ''; 
    this.collectionData = '';
    this.multiToOneData = '';
  }
  
  getCollectionJSON(apiURL) {
    /**
      * Method to get NOAA IR JSON. Relies on fetch API to requests JSON from 
      * NOAA IR JSON API. Uses if else condition to check
      * if apiURL contains "rows=" substring; if substring is present
      * method is queried once. 
      * 
      **/

    // method is utilized by URLS with already 
    // contain row num query string parameter
    // and can availble this request
    if (apiURL.includes("rows=") == false) { 

      var jsonOneRows = fetchAPI(apiURL);
      var colNum = jsonOneRows['response']['numFound'].toString();
      var url = apiURL + '?rows=' + colNum;
      var jsonAllRows = fetchAPI(url);
      var docs = jsonAllRows['response']['docs'];
      this.collectionJSON = docs;

    } else {

    var jsonAllRows = fetchAPI(apiURL);
    var docs = jsonAllRows['response']['docs'];
    this.collectionJSON = docs;

    }
  }
  
  getMemberItememCount(sheetNum,colNum, numCols) {
    /* get item count of queried collection method
    returns array of arrays */
    
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var sheet = ss.getSheets()[sheetNum];
    var lastRow = sheet.getLastRow();
    // row, col, numRows, numCols
    var dataRange = sheet.getRange(2,colNum, lastRow, numCols);
    var memberDataArray = (dataRange.getValues().join(';')).split(';');  

    
    //  call itemCount function to return count of PID in a JS object
    var count = itemCount(memberDataArray);

    return this.collectionMemberItemCount = count;

     } // end of getItemCount method
     
 
   getCollectionInfo(dataObj) {
     /** 
      * Method used to get NOAA IR collection info. 
      * takes in data object and returns array of arrays
      * noaa collection PIDs are also replaces with collection names
      * using a an if conditional
      * 
      **/
   
     var collectionArr = [['Name', 'Count']]
     
     for (const [colName, colPID] of Object.entries(dataObj)) {
       // item count 
       for (const [itemPID, itemCount] of Object.entries(this.collectionMemberItemCount)) {
         if (itemPID == colPID) {
           collectionArr.push([colName,itemCount])
           } 
         }
       }   
        collectionArr.push(getTotalValue(collectionArr));
        return collectionArr
    
   } // end of method
       

    convertObjectToArray(dataObj) {
    /**
     * Method to convert JS data object to a 2d array
     **/

    var collectionArr = [['Name', 'Count']]
      for (const [name, count] of Object.entries(dataObj)) {      
            collectionArr.push([name, count])
            } 
        
      // get total value of collection count. append to last row
      collectionArr.push(getTotalValue(collectionArr));  

      return collectionArr
    }

    multiToOne(sheetNum, idColNum, multiCol, delimiter) {
      /**
       * Method selects two columns from a sheet(tab). One
       * column is selected as the Id column. If other column 
       * has multiple values, the values are separated creating 
       * a new row. 
       * 
       * Returns a 2d array.
       **/

      var ss = SpreadsheetApp.getActiveSpreadsheet();
      var sheet = ss.getSheets()[sheetNum];
      var lastRow = sheet.getLastRow();
      
      // column serves as unique id for rows
      var idColRange = sheet.getRange(2,idColNum, lastRow, 1);
      // column with multivalue cells
      var multiRange = sheet.getRange(2, multiCol, lastRow, 1);

      var idDataArray = idColRange.getValues();
      var multiDataArray = multiRange.getValues();
      // column names

      var multiToOneArr = [['id','Name']];

      idDataArray.forEach(function(idItem, index) {
        var multiItemArray = multiDataArray[index][0].split(delimiter);
        var idItem = idItem[0]; 

        if (multiItemArray.length > 1) {
          
          multiItemArray.forEach(function(mItem) {
            multiToOneArr.push([idItem, mItem]);
          });
          
        } else if (multiItemArray.length <= 1) {
          multiItemArray.unshift(idItem); // ad idItem to beginning of arr
          multiToOneArr.push(multiItemArray);
        }
      });
      return this.multiToOneData = multiToOneArr
    }


   getFields() {
     /**
      * Method to retrievevv fields from JSON object.
      * 
      * Loops through JSON to select fields listed in method. 
      * As fields are hard coded into field, adding/deleting fields
      * must be done within method.
      * 
      * Returns a 2d Array, storing data in this.collectionData Query attribute. 
      **/
    
     var fields = [];
    
    // this.collection attribute stores returned JSON metadata
     for (var key of this.collectionJSON) {
       
        // Link
        var link = key['PID'].replace('noaa:','https://repository.library.noaa.gov/view/noaa/');
        var title = key['mods.title'];
        
        // create date created and format as JS date object
        var createDate = new Date(key['fgs.createdDate']);
        createDate = createDate.toISOString().substring(0, 10);
      
        // create modified date and format as JS date object 
        var modDate = new Date(key['fgs.lastModifiedDate']);
        modDate = modDate.toISOString().substring(0, 10);
        
        // retrieve document type
        var compliance = key['mods.sm_compliance']; 
        if (compliance == null) { compliance = [''];}
        compliance = compliance.join(' | ');
      
        // retrieve published year mods.ss_publishyear
        var pubYear = key['mods.ss_publishyear']; 
        if (pubYear == null) { pubYear = ['no published year info'];}
    
        // retrieve document type. 
        var docType = key['mods.type_of_resource']; 
        if (docType == null) { docType = [''];}
        docType = docType.join(';');
      
        // retrieve DOI
        var doi = key['mods.sm_digital_object_identifier']
        if (doi == null) {doi = [''];}
        doi = doi.join(";");

        // retieve creation date in MMYYYY format. 
        var createDateMMYYYY  = new Date(key['fgs.createdDate']); // format date 
        createDateMMYYYY = createDateMMYYYY.toISOString().substring(0, 7);

      // pass createDate object into fyChecker
      //function to return a FY## creation date
      var fy = fyChecker(createDate)

      // retrieve the collections an item belongs to
      var rdfMemberOf = key['rdf.isMemberOf'];
      rdfMemberOf = rdfMemberOf.join(';');

      // retrieve the facet info (program/office/LO/etc) associated w/ item
      var localCorpName = key['mods.sm_localcorpname'];
      if (localCorpName == null) { localCorpName = [''];}
      localCorpName = localCorpName.join(';');

      // retrieve series info associated with item
      var series = key['mods.related_series'];
      if (series == null) { series = [''];}
      series = series.join('~');

        fields.push([link, title, createDate, modDate, compliance, pubYear, docType,
      doi, createDateMMYYYY, fy, rdfMemberOf, localCorpName, series]);


      } /// end of for loop
   
    
   return this.collectionData = fields;
   } // end of getFields 
   
        
  
} // end of Query class


/**
 * FUNCTIONS SECTION
 * functions are utlized within Query class or
 * on Query class instances.
 **/


function fetchAPI(apiURL) {
  /**
   * Function to request NOAA IR REST API.
   * Returns JSON response.
   **/
   var response = UrlFetchApp.fetch(apiURL);
   var data = response.getContentText();
   var json = JSON.parse(data);
   return json;
}


function itemCount(array) {
  /** item count function
   * count occurances of PIDs
   * returns a JS object
  **/ 

  var count = {};
  array.forEach(val => count[val] = (count[val] || 0) + 1);
  return count;
}


function printToGoogleSheet(sheetNum, colNum, arrayData, soloOutput) {
  /**  prints collection data to Google Sheet 
   *  @param: sheetNum: Google Sheet (tab) to when data will be output. O based index
   *  @param: colNum: Column number where data will begin
   *  @param: arrayData: Array of Arrays to be output to Google Sheet
   *  @param: soloOutput: whether data is the only output on sheet
  **/

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheets()[sheetNum];

  if (soloOutput === true) {
    var lastRow = sheet.getLastRow();
  } else if (soloOutput === false) {
    var lastRow = 1;
  }

  // row, col, numRows, numCols
  var dataRange = sheet.getRange(lastRow + 1,colNum, arrayData.length, arrayData[0].length);
  dataRange.setValues(arrayData);
  
}

function iterateRows(apiURL, total, chunk_val) {
  /* 
  if total number of rows is larger than chunk_val
  a list of URLs is generated with url row num appended
  */

  // iterates at chunk_val
  if (total < chunk_val) {
    return apiURL;
  } else {
    // chunk number into parts
    var chunkArray = new Array(Math.floor(
      total / chunk_val)).fill(chunk_val).concat(total % chunk_val);

    // push 0 to beginning of chunkArray
    chunkArray.unshift(0);

    // add previous value of array to next
   var sumChunkArray = chunkArray.map((elem, index) => chunkArray.slice(0,index + 1).reduce((a, b) => a + b));

    var chunkLinkArray = [];
    for (const element of sumChunkArray) {
      // generate URLs with rows.
      if (element != total) {
        chunkLinkArray.push(apiURL + '?rows=' + chunk_val.toString() + '&start=' + element);
      } else {
        continue // continue if start == total
      }  
    }
    return chunkLinkArray;
  }
}

function fetcRowTotal(apiURL) {
  // fetch API URL row total

  var jsonOneRows = fetchAPI(apiURL);
  var colNum = jsonOneRows['response']['numFound'].toString();
  return colNum; 
}

function clearContent(sheetNum,clearContentRange) {
  // A1 Notation
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheets()[sheetNum];
  sheet.getRange(clearContentRange).clearContent();
}


function sortCollectionData(sheetNum, columnNum, sortOrder) {
  // sort collection data
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheets()[sheetNum];
  var lastRow = sheet.getLastRow();
  var lastColumn = sheet.getLastColumn();

  var dataRange = sheet.getRange(2,1, lastRow, lastColumn);

  dataRange.sort({column: columnNum, ascending: sortOrder});
}

// get total value, return as array

function getTotalValue(results) {
  /** 
   * Returns an array value of the sumResults of the values 
   * in an array. Function is utilized within a function. 
   * **/
  
  var sumResults = 0;
  
  for (var x=0; x < results.length; x++) {
    if (Number.isInteger(results[x][1]) == true) {
      sumResults += results[x][1];
      }
  }
 
  return ['Total', sumResults]; 
 }

function filterNull(dataArr) {

  finalArr = [];

  for (const [i, j] of dataArr) {
    if (j !== '') {
      finalArr.push([i,j]);
    }
  }
  return finalArr;
}

function fyChecker(date) {
  /**
   * Check what FY date falls under
   * 
   * Returns a FY## 
   **/

  // formats date string as JS Date Object 
  var createDate = new Date(date); 

  var fy16Begin = new Date('2016-10-01');
  var fy16End = new Date('2017-09-31');

  var fy17Begin = new Date('2017-10-01');
  var fy17End = new Date('2018-09-31');

  var fy18Begin = new Date('2018-10-01');
  var fy18End = new Date('2019-09-31');

  var fy19Begin = new Date('2018-10-01');
  var fy19End = new Date('2019-09-31');

  var fy20Begin = new Date('2019-10-01');
  var fy20End = new Date('2020-09-31');

  var fy21Begin = new Date('2020-10-01');
  var fy21End = new Date('2021-09-31');


  if (createDate > fy16Begin  && createDate < fy16End) {
    return 'FY16';
  }
  else if (createDate > fy17Begin  && createDate < fy17End) {
    return 'FY17';
  } else if (createDate > fy18Begin  && createDate < fy18End) {
    return 'FY18';
  } else if (createDate > fy19Begin  && createDate < fy19End) {
    return 'FY19'; 
} else if (createDate > fy20Begin  && createDate < fy20End) {
    return 'FY20';
  } else if (createDate > fy21Begin  && createDate < fy21End) {
    return 'FY21';
} else {
  return '';
  }
}

// IR collection PIDs
var collectionNamePIDInfo = {
  "National Environmental Policy Act (NEPA)": "noaa:1",
  "Coral Reef Conservation Program (CRCP)": "noaa:3",
  "Ocean Exploration Program": "noaa:4",
  "National Marine Fisheries Service (NMFS)":"noaa:5",
  "National Weather Service (NWS)": "noaa:6",
   "Office of Oceanic and Atmospheric Research (OAR)": "noaa:7",
   "National Ocean Service (NOS)":"noaa:8",
   "National Environmental Satellite and Data Information Service (NESDIS)": "noaa:9",
   "Sea Grant Publications": "noaa:11",
   "Education and Outreach": "noaa:12",
   "NOAA General Documents": "noaa:10031",
   "NOAA International Agreements": "noaa:11879",
   "Office of Marine and Aviation Operations (OMAO)": "noaa:16402",
   "Integrated Ecosystem Assessment": "noaa:22022",
   "NOAA Cooperative Institutes": "noaa:23649",
   "Weather Research and Forecasting Innovation Act": "noaa:23702",
   "NOAA Cooperative Science Centers": "noaa:24914" 
   }


// main function 
function callAPI() {
  var apiUrl = "https://repository.library.noaa.gov/fedora/export/view/collection/noaa:6";
  var chunkVal = 1500;
  var myQuery = new Query(apiUrl);
  apiURLs = iterateRows(apiUrl,myQuery.rowTotal, chunkVal);
  
  // sheetnames, numbers
  var allItemSheetNum = 0;
  var collectionSheetNum = 1;
  var facetSheetNum = 2;
  var serialsSheetNum = 3;
  
  // clear content of All Items tab before new data query
  clearContent(allItemSheetNum,"A2:M")
  if (apiURLs instanceof Array === true) {
    
    // if there are multiple API URLs
    for (const elem of apiURLs) {
      myQuery.getCollectionJSON(elem);
      myQuery.getFields();
      // print all IR metadata to sheet
      printToGoogleSheet(allItemSheetNum,1,myQuery.collectionData, true); 

    }
  } else if(typeof apiURLs == 'string') {
     // if there is a single API URL
    myQuery.getCollectionJSON(apiURLs);
    myQuery.getFields();

    // print all IR metadata to sheet
    printToGoogleSheet(allItemSheetNum,1,myQuery.collectionData, true);
 }

  // sorts all IR metadata by data created field
  sortCollectionData(allItemSheetNum, 3, true)

  clearContent(collectionSheetNum, "A1:B");
  myQuery.multiToOne(allItemSheetNum, 1, 11, ';');
  printToGoogleSheet(collectionSheetNum,1,myQuery.multiToOneData, true);

  // facet info
  clearContent(facetSheetNum, "A1:B");
  myQuery.multiToOne(allItemSheetNum, 1, 12,';'); // first, last column
  printToGoogleSheet(facetSheetNum,1,myQuery.multiToOneData, true);

  // serial sheet
  clearContent(serialsSheetNum, "A1:B");
  myQuery.multiToOne(allItemSheetNum, 1, 13,'~')

  printToGoogleSheet(serialsSheetNum,1,filterNull(myQuery.multiToOneData), true);
 
}