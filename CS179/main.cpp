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

#define NUM_THREADS 5
int globalFlag = 0; 
int current_thread = 0; 
void *searchQuery(vector<Database*>* DB, string collChoose,string docInput ,string DBchoose,Collection *coll)
{   
    int num = current_thread++; 
    string  keyName, objName, attName;
    int count, type, MAX,matches=0, results=0;
    Document d, d2;
    MAX = coll->getDocs().size(); 
    const char *docToC = docInput.c_str(); 
    d.Parse(docToC);
    count = d.MemberCount(); 
    cout << current_thread << endl; 
    
    //Checking if the Collection is empty 

        for (int i = num *(MAX/NUM_THREADS); i < ((num + 1) *(MAX/NUM_THREADS)); i++){
            bool objAttFlag = false;
            matches = 0;     
            d2.CopyFrom(*coll->getDocAt(i), d2.GetAllocator()); //d2 copies current document* data
            for (Value::ConstMemberIterator iter = d.MemberBegin(); iter != d.MemberEnd(); ++iter){
                //if key value has '.' in it
                keyName = iter->name.GetString();
                if(keyName.find('.') < keyName.length()){
                    objAttFlag = true;
                    objName = keyName.substr(0, keyName.find('.'));
                    attName = keyName.substr(keyName.find('.')+1, keyName.length()-keyName.find('.'));
                }
                if(objAttFlag){
                    const char *objN = objName.c_str();
                    const char *attN = attName.c_str();
                    if(d2.HasMember(objN)){
                        Value& valOfExist = d2[objN]; //this will be an object
                        if (valOfExist.HasMember(attN)){
                            Value& valOfMemberInExistObj = valOfExist[attN];
                            type = valOfMemberInExistObj.GetType();
                            switch(type){
                                case 0:{
                                    matches++;
                                    break;
                                }
                                case 1:{
                                    matches++;
                                    break;
                                }
                                case 2:{
                                    matches++;
                                    break;
                                }
                                case 3: {
                                    break;
                                }
                                case 4: {
                                    bool flag = true;
                                    int globalFlag = 1; 
                                    Value& tempArr = d[iter->name.GetString()]; 
                                    if (tempArr.Size() == valOfMemberInExistObj.Size()){
                                        for (int i = 0; i < valOfMemberInExistObj.Size(); i++){
                                            if (tempArr[i] != valOfMemberInExistObj[i]){
                                                flag = false;
                                            }
                                        }
                                    } else {
                                        flag = false;
                                    }
                                    if (flag){
                                        matches++;
                                    }
                                    break;
                                }
                                case 5:{
                                    string temp1 = iter->value.GetString();
                                    string temp2 = valOfMemberInExistObj.GetString();
                                    if (temp1 == temp2){
                                        matches++;
                                    }
                                    break;
                                }
                                case 6:{
                                    if (iter->value.GetDouble() == valOfMemberInExistObj.GetDouble()){
                                        matches++;
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }

                //keymember has no '.'
                Value& val = d[iter->name.GetString()]; 
                type = val.GetType();
                if (d2.HasMember(iter->name.GetString())){ //check if current doc has key member matching input key
                    Value& val2 = d2[iter->name.GetString()];
                    if (val.GetType() == val2.GetType()){
                        switch(type){
                            case 0:{ //type: null
                                matches++;
                                break;
                            }
                            case 1:{ //type: false
                                matches++;
                                break;
                            }
                            case 2:{ //type: true
                                matches++;
                                break;
                            }
                            case 3:{ //type: object
                                string inputTemp, docTemp;
                                int typeTemp;
                                count = val.MemberCount();
                                Value::ConstMemberIterator docItr = val2.MemberBegin();
                                for (Value::ConstMemberIterator inputItr = val.MemberBegin(); inputItr != val.MemberEnd(); ++inputItr) {   //iterate through object
                                    inputTemp = inputItr->name.GetString();
                                    docTemp = docItr->name.GetString();
                                    if(inputTemp == docTemp){ //if key names match
                                        Value& inputVal = val[inputItr->name.GetString()];
                                        Value& docVal = val2[docItr->name.GetString()];
                                        if (inputVal.GetType() == docVal.GetType()){
                                            typeTemp = inputVal.GetType();
                                            switch(typeTemp){
                                                case 0: {//type:null
                                                    matches++;
                                                    break;
                                                }
                                                case 1: { //type: false
                                                    matches++;
                                                    break;
                                                }
                                                case 2: { //type: true
                                                    matches++;
                                                    break;
                                                }
                                                case 3: { //type: object
                                                    break;
                                                }
                                                case 4: { //type: array
                                                    bool flag = true;
                                                    globalFlag = 1; 
                                                    if (inputVal.Size() == docVal.Size()){
                                                        for (int i = 0; i < docVal.Size(); i++){
                                                            if (inputVal[i] != docVal[i]){
                                                                flag = false;
                                                            }
                                                        }
                                                    } else {
                                                        flag = false;
                                                    }
                                                    if (flag){
                                                        matches++;
                                                    }
                                                    break;
                                                }
                                                case 5: {//type: string
                                                    string temp1 = inputItr->value.GetString();
                                                    string temp2 = docItr->value.GetString();
                                                    if (temp1 == temp2){
                                                        matches++;
                                                    }
                                                    break;
                                                }
                                                case 6:{ //type: number
                                                    if (inputItr->value.GetDouble() == docItr->value.GetDouble()){
                                                        matches++;
                                                    }
                                                    break;
                                                }
                                                default:{
                                                    break;
                                                }
                                            } //end switch
                                        } else{ // else if val types !match break loop
                                            break;
                                        }
                                    } else { // else if key names !match break loop
                                        break;
                                    }
                                    docItr++;
                                }
                                break;
                            }
                            case 4: {//type: array
                                bool flag = true;
                                if (val.Size() == val2.Size()){
                                    for (int i = 0; i < val2.Size(); i++){
                                        if (val[i] != val2[i]){
                                            flag = false;
                                        }
                                    }
                                } else {
                                    flag = false;
                                }
                                if (flag){
                                    matches++;
                                }
                                
                                break;
                            }
                            case 5: { // type: string
                                string temp1 = iter->value.GetString();
                                string temp2 = val2.GetString();
                                if (temp1 == temp2){
                                    matches++;
                                }
                                break;
                            }
                            case 6:{ //type: number
                                if (iter->value.GetDouble() == val2.GetDouble()){
                                    matches++;
                                }
                                break;
                            }
                            default: {
                                cout << "Error in switch" << endl;
                                break;
                            }
                        }
                    } else {
                        break; //break out of this loop if key member val type does not match input val type
                    }
                } else {
                    break; //break out of this loop if 
                }
            } //end of for loop for inputted members
            if (matches == count){
                results++;
                cout << endl << "Database: " << DB->at(stoi(DBchoose))->getName() << endl <<  "  Collection: " << DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->getName();
                cout << endl << "    Document #" << i << endl;
                Document temp;
                StringBuffer buffer;
                Writer<StringBuffer> writer(buffer);
                DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->getDocAt(i)->Accept(writer);
                cout << "\t" << buffer.GetString() << endl;
            }
        } // end of for loop for documents in the collection
        cout << "Total Matches Found: " << results << endl;
        
     
}
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
              
              
                const int number_threads = 10; 
                vector<thread> threads; 
            
                
                for(int i=0;i< number_threads;i++){
                    
                    //threads[i]= thread(inputManager.searchQuery,&allDatabases, collChoose, docInput, DBchoose,coll,block_start,block_end);
                    threads.push_back(thread(searchQuery,&allDatabases, collChoose, docInput, DBchoose,coll)); 
                    
                }
                for(int i=0;i< number_threads;i++){
                    threads[i].join(); 
                }

            

                //inputManager.searchQuery(&allDatabases, collChoose, docInput, DBchoose,coll);
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


