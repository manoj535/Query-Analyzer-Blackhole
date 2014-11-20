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

MYSQL *mysql;
MYSQL_RES *results;
MYSQL_ROW row, end_row;
MYSQL_FIELD *field;
#define setQuery "set count of "

static char *server_options[] = \
  { "mysql_test", 
    "--defaults-file=my.init", 
    NULL };

int num_elements = (sizeof(server_options) / sizeof(char *)) - 1;

static char *server_groups[] = { "server",
                                 NULL };


int run_query(MYSQL *mysql, const char *query);

// starts the embedded mode mysql server
int initialize_mysql();

int close_mysql();

int display_results();

int queralyzer(char *buf) 
{
  int needsCleanup = 0;
  needsCleanup = initialize_mysql();
  run_query(mysql, buf);
  display_results();
  if (needsCleanup) 
  {
    close_mysql();
  }
  return 0;
}

int initialize_mysql()
{
   mysql_library_init(num_elements, server_options, server_groups);
   mysql = mysql_init(NULL);
   if (mysql) 
   {
     mysql_options(mysql, MYSQL_READ_DEFAULT_GROUP, "server");
     mysql_options(mysql, MYSQL_OPT_USE_EMBEDDED_CONNECTION, NULL);
     if (!mysql_real_connect(mysql, NULL,NULL,NULL, "sampledb_blackhole", 0,NULL,0)) 
     {
       printf("mysql_real_connect failed: %s \n", mysql_error(mysql));
       return -1;
     }  
     return 0;
   } 
   else 
   {
     printf("mysql was never inited succesfully\n");
     return -1;
   }
} // initialize_mysql

int run_query(MYSQL *mysql, const char *query) {
	printf("run_query\n");
  if (mysql_query(mysql, query)) 
  {
    printf("problems running %s , error %s \n", query, mysql_error(mysql));
    return 1;
  } else 
  {
    return 0;
  }
} // run_query

/**
 * Displays results of the previously run query, unfortunately mysql is kinda global, 
 * so have to query it to get all details rather than parameter passing 
 */
int display_results() 
{
	printf("display_results\n");
	// select or update based on the field count
	if (mysql_field_count(mysql) > 0) {
    int num_fields, i;
    results = mysql_store_result(mysql);
    
    // used to determine if the query returned any results
    if (results) 
    {
		num_fields = mysql_num_fields(results);

      // print the columns
      for( i = 0; field = mysql_fetch_field(results), i < num_fields; i++) {
        printf("%s\t", field->name?field->name: "NULL"); 
      }
      printf("\n");

      // print the values
      while((row = mysql_fetch_row(results))) {
        for (end_row = row + num_fields; row < end_row; ++row) {
          printf("%s\t", row ? (char*)*row : "NULL"); 
        }
        printf("\n");
      }
      mysql_free_result(results);
      return 0;
    } else 
    {
      printf("Could not get any results\n");
      return 1;
    }
  } 
  else 
  {
    // update/insert so only rows impacted count available
    printf("Affected rows: %lld\n", mysql_affected_rows(mysql));
    return 0;
  }
}

int close_mysql() 
{
     mysql_close(mysql);
     mysql_library_end();
     return 0;
}

int main()
{
	std::string lQuery;	
	printf("enter query:\n");
	std::getline(std::cin, lQuery);
	if(lQuery.find(setQuery) == std::string::npos)
	{
		// set eq_range_index_dive_limit to 0
		std::string lSetIndexDiveLimit = "set session eq_range_index_dive_limit=0;";
		queralyzer((char *)lSetIndexDiveLimit.c_str());
		std::cout<<lQuery<<std::endl;
		queralyzer((char *)lQuery.c_str());
	}
	else
	{
		// if query is "set count of <table>=<count>"
		long lCount=0;
		std::string lTable;
		size_t lFound;
		lFound = lQuery.find("=");
		lTable = lQuery.substr(13,lFound-13);
		lQuery=lQuery.substr(lFound+1);
		lCount = atoi(lQuery.c_str());
		std::string lLine;
		bool lFoundTable=false;
		std::fstream  lRowCountFile;
		lRowCountFile.open("/tmp/rowcount.txt", std::ios::in | std::ios::out);
		if(!lRowCountFile.good())
		{
			lRowCountFile.open("/tmp/rowcount.txt", std::ios::out );
		}
		while(std::getline(lRowCountFile, lLine))
		{
			lFound=lLine.find(lTable);
			if(lFound!=std::string::npos)
			{
				lRowCountFile.seekg(-(lLine.length()+1),std::ios::cur);
				lRowCountFile<<lTable<<"="<<lCount<<std::endl;
				lFoundTable=true;
				break;
			}
		}
		if(!lFoundTable)
		{
			lRowCountFile.clear();
			//lRowCountFile.seekg(0,std::ios::beg);
			lRowCountFile<<lTable<<"="<<lCount<<std::endl;
		}
		lRowCountFile.close();
		//std::cout<<"table:"<<lTable<<","<<lCount<<std::endl;
		
	}
	return 0;	    
}
