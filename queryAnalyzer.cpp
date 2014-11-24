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
#define setQuery "set count of table "
#define rowCountFilePath "/tmp/rowcount.txt"
#define tableCount 19

bool run_query(MYSQL *mysql, const char *query);

// starts the embedded mode mysql server
bool initialize_mysql();

int close_mysql();

int display_results();

bool queralyzer(char *iBuf) 
{
	if(!initialize_mysql())
	return false;
	if(!run_query(gMySqlObj, iBuf))
	return false;
	display_results();
	close_mysql();
	return true;
}

bool initialize_mysql()
{
	static char *lServerOptions[] = { "mysql_test", "--defaults-file=my.init", NULL };
	int lNumOfElements = (sizeof(lServerOptions) / sizeof(char *)) - 1;
	static char *lServerGroups[] = { "server", NULL };
	mysql_library_init(lNumOfElements, lServerOptions, lServerGroups);
	gMySqlObj = mysql_init(NULL);
	if (gMySqlObj) 
	{
		mysql_options(gMySqlObj, MYSQL_READ_DEFAULT_GROUP, "server");
		mysql_options(gMySqlObj, MYSQL_OPT_USE_EMBEDDED_CONNECTION, NULL);
		if (!mysql_real_connect(gMySqlObj, NULL,NULL,NULL, "sampledb_fakeengine", 0,NULL,0)) 
		{
			printf("mysql_real_connect failed: %s \n", mysql_error(gMySqlObj));
			return false;
		}  
		return true;
	} 
	else 
	{
		printf("mysql was never inited succesfully\n");
		return false;
	}
} // initialize_mysql

bool run_query(MYSQL *iMySql, const char *iQuery) 
{
	if (mysql_query(iMySql, iQuery)) 
	{
		printf("problems running %s , error %s \n", iQuery, mysql_error(iMySql));
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
	MYSQL_RES *lResults;
	MYSQL_ROW lRow, lEndRow;
	MYSQL_FIELD *lField;
	//printf("display_results\n");
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
			printf("%s\t", lField->name?lField->name: "NULL"); 
		}
		printf("\n");

		// print the values
		while((lRow = mysql_fetch_row(lResults))) 
		{
			for (lEndRow = lRow + lNumOfFields; lRow < lEndRow; ++lRow) 
			{
				printf("%s\t", lRow ? (char*)*lRow : "NULL"); 
			}
			printf("\n");
		}
		mysql_free_result(lResults);
		return 0;
    } 
    else 
	{
		printf("Could not get any results\n");
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
     return 0;
}

bool checkTableInDatabase(std::string iTableName)
{
	string lQuery = "desc "+iTableName;
	if(!initialize_mysql())
	return false;
	if(!run_query(gMySqlObj, (char *)lQuery.c_str()))
	return false;
	close_mysql();
	return true;
}

int main()
{
	string lQuery;	
	// set eq_range_index_dive_limit to 0
	string lSetIndexDiveLimit = "set session eq_range_index_dive_limit=0;";
	queralyzer((char *)lSetIndexDiveLimit.c_str());
	while(1)
	{
		getline(cin, lQuery);
		if(lQuery=="quit")
		break;
		if(lQuery.find(setQuery) == string::npos)
		{
			if(lQuery.length()>1)
			queralyzer((char *)lQuery.c_str());
		}
		else
		{
			map<string,int> lTableMap;
			// if query is "set count of table <table>=<count>"
			long lCount=0;
			string lTable;
			size_t lFound;
			lFound = lQuery.find("=");
			lTable = lQuery.substr(tableCount,lFound-tableCount);
			// check if the table is present in db
			if(!checkTableInDatabase(lTable))
			{
				cout<<"Table not found in db"<<endl;
				return -1;
			} // if
			lQuery=lQuery.substr(lFound+1);
			lCount = atoi(lQuery.c_str());
			string lLine;
			string lTableInFile;
			long lCountInFile=0;
			bool lFoundTable=false;
			fstream  lRowCountFile;
			lRowCountFile.open(rowCountFilePath, ios::in | ios::out);
			if(!lRowCountFile.good())
			{
				// if row count file not present
				lRowCountFile.open(rowCountFilePath, ios::out );
				lRowCountFile<<lTable<<"="<<lCount<<endl;
				lRowCountFile.close();
				return 0;
			}
			// populate lTableMap
			while(getline(lRowCountFile, lLine))
			{
				lFound = lLine.find("=");
				lTableInFile = lLine.substr(0,lFound);
				lCountInFile = atoi((lLine.substr(lFound+1).c_str()));
				lTableMap.insert(pair<string,int>(lTableInFile, lCountInFile));
				
			} // while
			lRowCountFile.close();
			//lRowCountFile.clear();
			//lRowCountFile.seekg(0,ios::beg);
			map<string, int>::iterator lTableMapIterator;
			lRowCountFile.open(rowCountFilePath, ios::out );
			for(lTableMapIterator=lTableMap.begin();lTableMapIterator!=lTableMap.end();++lTableMapIterator)
			{
				if(lTable == lTableMapIterator->first)
				{
					lRowCountFile<<lTable<<"="<<lCount<<std::endl;
					lFoundTable=true;
				}
				else
					lRowCountFile<<lTableMapIterator->first<<"="<<lTableMapIterator->second<<std::endl;
			} // for
		
			if(!lFoundTable)
			{
				lRowCountFile<<lTable<<"="<<lCount<<std::endl;
			}
			lRowCountFile.close();
		} // else
	} // while
	return 0;	    
}
