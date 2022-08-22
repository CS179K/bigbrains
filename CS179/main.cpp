#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <sstream>
#include <fstream>
#include <cstring>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include <cstdio>
#include <thread>
#include <filesystem>
#include <math.h>
#include "inputHandler.h"
#include "database.h"
#include "rapidjson/ostreamwrapper.h"
using namespace std;
using namespace rapidjson;
using std::filesystem::directory_iterator;

int main(){
    vector<Database*> allDatabases;
    InputHandler inputManager;

    inputManager.readData(&allDatabases);
    
    bool complete = false;
	while (!complete)
	{
        int option = inputManager.displayMenu();
        if (option == 1){
            int addOpt = inputManager.displayAddMenu();
            if (addOpt == 1){
                inputManager.addDoc(&allDatabases);
            }
            else if (addOpt == 2){
                inputManager.addColl(&allDatabases);
            }
            else if (addOpt == 3){
                inputManager.addDB(&allDatabases);
            }
        }
        else if (option == 2){
            int remOpt = inputManager.displaySubMenu();
            if (remOpt == 1){
                inputManager.removeDoc(&allDatabases);
            }
            else if (remOpt == 2){
                inputManager.removeColl(&allDatabases);
            }
            else if (remOpt == 3){
                inputManager.removeDB(&allDatabases);
            }
        }
        else if (option == 3){
            int updateOpt = inputManager.displayUpdateMenu();
            if (updateOpt == 1){
                //inputManager.updateDoc(&allDatabases);
            }
            else if (updateOpt == 2){
                inputManager.updateColl(&allDatabases);
            }
            else if (updateOpt == 3){
                inputManager.updateDB(&allDatabases);
            }
        }
        else if (option == 4){ //search
            string DBchoose, collChoose, docInput, keyName, objName, attName;
            int count, type, matches=0, results=0;
            const int min_per_thread= 3;
            cout << "Choose a database: " << endl;
            for (int i = 0; i < allDatabases.size(); i++){
                cout << i << ". " << allDatabases.at(i)->getName() << endl;
                }


            getline(cin, DBchoose);
            if (!allDatabases.at(stoi(DBchoose))->getCollections().empty()){
                cout << "Choose a collection: " << endl;
                allDatabases.at(stoi(DBchoose))->print();
                getline(cin, collChoose);
                cout << "Enter a document. EX: { \"name\" : \"michael\" }" << endl;
                getline(cin, docInput);
            
                Collection* coll = new Collection();
                coll = allDatabases.at(stoi(DBchoose))->getCollection(stoi(collChoose));
                const int length = coll->getDocs().size();
                const int max_threads = ceil((length)/(float)min_per_thread); 

                inputManager.searchQuery(&allDatabases, collChoose, docInput, DBchoose,coll);
            }
            else{
                 cout << "No collections in this database. " << endl;
        }
        }
        else if (option == 5){ //print
            for (int i = 0; i < allDatabases.size(); i++){
                allDatabases.at(i)->printAll();
            }
        }
        else if (option == 6){
            complete = true;
        }
    }
    return 0;

}   


