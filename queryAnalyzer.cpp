#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include "mysql.h"
#include <dlfcn.h>
#include <vector>
#include <string>
#include <iostream>
#include <fstream>
#include <map>
using namespace std;

MYSQL *gMySqlObj;
#define kSetQuery "set count of table "
#define kSetIndexRangeQuery "set range of index "
#define kSetIndexCardinalityQuery "set count of index "
#define kRowCountFilePath "/tmp/rowcount.txt"
#define kIndexCountFilePath "/tmp/indexcount.txt"
#define kIndexRangeFilePath "/tmp/indexrangecount.txt"
#define kIsEqual "="
#define kComma ","
#define kFindTable "from"
#define kTableCount 19

bool run_query(MYSQL *mysql, const char *query);
bool checkTableInDatabase(std::string iTableName);

// starts the embedded mode mysql server
bool initialize_mysql();

int close_mysql();

int display_results();

bool processSetCountTableQuery(string iQuery);

bool processSetCountIndexQuery(string iIndexFilePath, string iQuery);

bool queralyzer(char *iQuery) 
{
	string lQuery;
	if(!initialize_mysql())
	return false;
	if(iQuery!=NULL)
	{
		// If queryanalyzer is invoked from http
		string lQuery = string(iQuery);
		if(lQuery[0]=='"' && lQuery[lQuery.length()-1] == '"') 
		{
			// remove the extra quotes
			lQuery=lQuery.substr(1,lQuery.length()-2);
		}
		if(lQuery.find(kSetQuery) != string::npos)
		{
			// if query is "set count of table <tablename>=<count>"
			processSetCountTableQuery(lQuery);
		} // if
		else if(lQuery.find(kSetIndexCardinalityQuery) != string::npos)
		{
			// if query is "set count of index <indexname> from <tablename>=<count>"
			processSetCountIndexQuery(kIndexCountFilePath, lQuery);
		}
		else if(lQuery.find(kSetIndexRangeQuery) != string::npos)
		{
			// if query is "set range of index <indexname> from <tablename>=<count>"
			processSetCountIndexQuery(kIndexRangeFilePath, lQuery);
		}
		else
		{
			if(lQuery.length()>1)
			run_query(gMySqlObj, lQuery.c_str());
			display_results();
		}
	}
	else
	{
		// if queryanalyzer is invoked on local machine
		while(1)
		{
			//fflush(stdin);
			getline(cin, lQuery, ';');
			cin.ignore();
			if(lQuery=="quit")
			break;
			
			if(lQuery.find(kSetQuery) != string::npos)
			{
				// if query is "set count of table <tablename>=<count>"
				processSetCountTableQuery(lQuery);
			} // if
			else if(lQuery.find(kSetIndexCardinalityQuery) != string::npos)
			{
				// if query is "set count of index <indexname> from <tablename>=<count>"
				processSetCountIndexQuery(kIndexCountFilePath, lQuery);
			}
			else if(lQuery.find(kSetIndexRangeQuery) != string::npos)
			{
				// if query is "set range of index <indexname> from <tablename>=<count>"
				processSetCountIndexQuery(kIndexRangeFilePath, lQuery);
			}
			else
			{
				if(lQuery.length()>1)
				run_query(gMySqlObj, lQuery.c_str());
				display_results();
			}
		} // while
    } // else
	close_mysql();
	return true;
}

bool initialize_mysql()
{
	string kDatabaseName = "sampledb_fakeengine";
	static char *lServerOptions[] = { "mysql_test", "--defaults-file=/opt/queralyzer/my.init", NULL };
	int lNumOfElements = (sizeof(lServerOptions) / sizeof(char *)) - 1;
	static char *lServerGroups[] = { "server", NULL };
	mysql_library_init(lNumOfElements, lServerOptions, lServerGroups);
	gMySqlObj = mysql_init(NULL);
	if (gMySqlObj) 
	{
		mysql_options(gMySqlObj, MYSQL_READ_DEFAULT_GROUP, "server");
		mysql_options(gMySqlObj, MYSQL_OPT_USE_EMBEDDED_CONNECTION, NULL);
		if ( !mysql_real_connect(gMySqlObj, NULL, NULL, NULL, kDatabaseName.c_str(), 0, NULL, 0))
		{
			cout<<"mysql_real_connect failed:"<<mysql_error(gMySqlObj)<<endl;
			return false;
		}  
		return true;
	} 
	else 
	{
		cout<<"mysql was never inited succesfully"<<endl;
		return false;
	}
} // initialize_mysql

bool run_query(MYSQL *iMySql, const char *iQuery) 
{
	if (mysql_query(iMySql, iQuery)) 
	{
		cout<<"problems running :"<< iQuery<<", error :"<<mysql_error(iMySql)<<endl;
		return false;
	} 
	else 
	{
		return true;
	}
} // run_query

/**
 * Displays results of the previously run query, unfortunately mysql is kinda global, 
 * so have to query it to get all details rather than parameter passing 
 */
int display_results() 
{
	MYSQL_RES *lResults = NULL;
	MYSQL_ROW lRow, lEndRow;
	MYSQL_FIELD *lField = NULL;
	// select or update based on the field count
	if (mysql_field_count(gMySqlObj) > 0) {
    int lNumOfFields, i;
    lResults = mysql_store_result(gMySqlObj);
    
    // used to determine if the query returned any results
    if (lResults) 
    {
		lNumOfFields = mysql_num_fields(lResults);

		// print the columns
		for( i = 0; lField = mysql_fetch_field(lResults), i < lNumOfFields; i++) 
		{
			printf("%s\t",lField->name?lField->name: "NULL");
		}
		
		cout<<endl;
		// print the values
		while((lRow = mysql_fetch_row(lResults))) 
		{
			for (lEndRow = lRow + lNumOfFields; lRow < lEndRow; ++lRow) 
			{
				printf("%s\t",lRow ? (char*)*lRow : "NULL");
			}
			cout<<endl;
		}
		mysql_free_result(lResults);
		return 0;
    } 
    else 
	{
		cout<<"Could not get any results"<<endl;
		return 1;
	}
  } 
  else 
  {
    // update/insert so only rows impacted count available
    //printf("Affected rows: %lld\n", mysql_affected_rows(mysql));
    return 0;
  }
}

int close_mysql() 
{
     mysql_close(gMySqlObj);
     mysql_library_end();
     gMySqlObj = NULL;
     return 0;
}

bool checkTableInDatabase(std::string iTableName)
{
	bool lResult = true;
	MYSQL_RES *lResultOfQuery = NULL;
	string lQuery = "desc "+iTableName;
	if(!run_query(gMySqlObj, (char *)lQuery.c_str()))
	lResult=false;
	// free the results as we dont need to display it
	lResultOfQuery = mysql_store_result(gMySqlObj);
	mysql_free_result(lResultOfQuery);
	return lResult;
}


bool processSetCountTableQuery(string iQuery)
{
	map<string,int> lTableMap;
	// if query is "set count of table <table>=<count>"
	long lCount=0;
	string lTable;
	size_t lFound;
	lFound = iQuery.find(kIsEqual);
	lTable = iQuery.substr(kTableCount,lFound-kTableCount);
	// check if the table is present in db
	if(!checkTableInDatabase(lTable))
	{
		cout<<"Table not found in db"<<endl;
		return false;
	} // if
			
	iQuery=iQuery.substr(lFound+1);
	lCount = atoi(iQuery.c_str());
	string lLine;
	string lTableInFile;
	long lCountInFile=0;
	bool lFoundTable=false;
	fstream  lRowCountFile;
	lRowCountFile.open(kRowCountFilePath, ios::in | ios::out);
	if(!lRowCountFile.good())
	{
		// if row count file not present
		lRowCountFile.open(kRowCountFilePath, ios::out );
		lRowCountFile<<lTable<<kIsEqual<<lCount<<endl;
		lRowCountFile.close();
		cout<<"set count of table "<<lTable<<" to "<<lCount<<" successfull"<<endl;
		return true;
	}
	// populate lTableMap
	while(getline(lRowCountFile, lLine))
	{
		lFound = lLine.find(kIsEqual);
		lTableInFile = lLine.substr(0,lFound);
		lCountInFile = atoi((lLine.substr(lFound+1).c_str()));
		lTableMap.insert(pair<string,int>(lTableInFile, lCountInFile));
				
	} // while
	lRowCountFile.close();
	//lRowCountFile.clear();
	//lRowCountFile.seekg(0,ios::beg);
	map<string, int>::iterator lTableMapIterator;
	lRowCountFile.open(kRowCountFilePath, ios::out );
	for(lTableMapIterator=lTableMap.begin();lTableMapIterator!=lTableMap.end();++lTableMapIterator)
	{
		if(lTable == lTableMapIterator->first)
		{
			lRowCountFile<<lTable<<kIsEqual<<lCount<<endl;
			lFoundTable=true;
		}
		else
		{
			lRowCountFile<<lTableMapIterator->first<<kIsEqual<<lTableMapIterator->second<<endl;
		}
	} // for
		
	if(!lFoundTable)
	{
		lRowCountFile<<lTable<<kIsEqual<<lCount<<endl;
	}
	cout<<"set count of table "<<lTable<<" to "<<lCount<<" successfull"<<endl;
	lRowCountFile.close();
	return true;
}

bool processSetCountIndexQuery(string iIndexFilePath, string iQuery)
{
	map<pair<string,string>, int> lIndexMap;
	// if query is "set count of index <indexname> from <tablename>=<count>"
	long lCount=0;
	string lTable, lIndex;
	size_t lFound,lFound2;
	lFound = iQuery.find(kIsEqual);
	lFound2 = iQuery.find(kFindTable);
	lIndex = iQuery.substr(kTableCount,lFound2-kTableCount-1);
	lTable = iQuery.substr(lFound2+5,lFound-lFound2-5);
	
	// check if the table is present in db
	if(!checkTableInDatabase(lTable))
	{
		cout<<"Table not found in db"<<endl;
		return false;
	} // if
			
	iQuery=iQuery.substr(lFound+1);
	lCount = atoi(iQuery.c_str());
	string lLine;
	string lTableInFile;
	long lCountInFile=0;
	bool lFoundTable=false;
	fstream  lIndexCountFile;
	lIndexCountFile.open(iIndexFilePath.c_str(), ios::in | ios::out);
	if(!lIndexCountFile.good())
	{
		// if index count file not present
		lIndexCountFile.open(iIndexFilePath.c_str(), ios::out );
		lIndexCountFile<<lTable<<kComma<<lIndex<<kIsEqual<<lCount<<endl;
		cout<<"set count of index "<<lIndex<<" from table "<<lTable<<" to "<<lCount<<" successfull"<<endl;
		lIndexCountFile.close();
		return true;
	}
	size_t lFoundComma;
	string lIndexInFile;
	// populate lIndexMap
	while(getline(lIndexCountFile, lLine))
	{
		lFound = lLine.find(kComma);
		lTableInFile = lLine.substr(0,lFound);
		lFoundComma = lLine.find(kIsEqual);
		lIndexInFile = lLine.substr(lFound+1,lFoundComma-lFound-1);
		lCountInFile = atoi((lLine.substr(lFoundComma+1).c_str()));
		lIndexMap.insert(pair<pair<string, string>, int>(make_pair(lTableInFile, lIndexInFile), lCountInFile));
				
	} // while
	lIndexCountFile.close();
	//lRowCountFile.clear();
	//lRowCountFile.seekg(0,ios::beg);
	map<pair<string, string>, int>::iterator lIndexMapIterator;
	lIndexCountFile.open(iIndexFilePath.c_str(), ios::out );
	for(lIndexMapIterator=lIndexMap.begin();lIndexMapIterator!=lIndexMap.end();++lIndexMapIterator)
	{
		if(lTable == (lIndexMapIterator->first).first && lIndex==(lIndexMapIterator->first).second)
		{
			lIndexCountFile<<lTable<<kComma<<lIndex<<kIsEqual<<lCount<<std::endl;
			lFoundTable=true;
		}
		else
			lIndexCountFile<<(lIndexMapIterator->first).first<<kComma<<(lIndexMapIterator->first).second<<kIsEqual<<
								lIndexMapIterator->second<<std::endl;
	} // for
		
	if(!lFoundTable)
	{
		lIndexCountFile<<lTable<<kComma<<lIndex<<kIsEqual<<lCount<<std::endl;
	}
	cout<<"set count of index "<<lIndex<<" from table "<<lTable<<" to "<<lCount<<" successfull"<<endl;
	lIndexCountFile.close();
	return true;
}

int main(int argc, char **argv)
{
	int lResult = 0;
	// start query analyzer
	if(argc>1)
	queralyzer(argv[1]);
	else
	{
		if(!queralyzer(NULL))
		lResult = -1;
	}
	return lResult;	    
}
