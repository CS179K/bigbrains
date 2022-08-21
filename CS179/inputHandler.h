#pragma once

#include <iostream>
#include "collection.h"
#include <string>
#include "direct.h" //uncomment this when using windows
//if on mac, change all "\" to "/" to fix assertion error
//#include "unistd.h" //use this on MAC
#include <sys/types.h>
#include <sys/stat.h>
#include <fstream>
#include <filesystem>
#include <vector>
#include <thread>
#include <chrono>
#include <functional>
#include <mutex>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "rapidjson/prettywriter.h"
#include "database.h"

using namespace std;
using std::filesystem::directory_iterator;
using namespace rapidjson;
using namespace std::chrono;


class InputHandler{
public:
    int displayMenu();
    int displayAddMenu();
    int displaySubMenu();
    int displayUpdateMenu();
    void addDB(vector<Database*>*);
    void addColl(vector<Database*>*);
    void addDoc(vector<Database*>*);
    void readData(vector<Database*>*);
    void removeDB(vector<Database*>*);
    void removeColl(vector<Database*>*);
    void removeDoc(vector<Database*>*);
    void updateDB(vector<Database*>*);
    void updateColl(vector<Database*>*);
    void searchQuery(vector<Database*>*);

    void threadRead(vector<Database*>*);
    void singleRead(vector<Database*>*, string);
    void singleCollRead(vector<Database*>*, string, string);
    void fillPaths();

    vector<thread> DB_threads;
    vector<thread> Coll_threads;
    vector<string> DBPaths;
    vector<string> CollPaths;
    int pathCount = 0;

};

//displays main menu for user and returns chosen option 
int InputHandler::displayMenu()
{
    std::string str;
    int option;
	cout << "Please select an option." << endl;
	cout << "1. Add" << endl;
	cout << "2. Remove" << endl;
	cout << "3. Update" << endl;
    cout << "4. Query" << endl;
    cout << "5. thread" << endl;
    cout << "6. Print" << endl;
    cout << "7. Quit" << endl;
    getline(cin, str);
    option = stoi(str);
	return option;
}

//if add is chosen, display the ADD menu and return chosen option
int InputHandler::displayAddMenu(){
    std::string str;
    int option;
    cout << "Please select an option:" << endl;
    cout << "1. Add a document" << endl;
    cout << "2. Add a collection" << endl;
    cout << "3. Add a database" << endl;
    getline(cin,str);
    option = stoi(str);
    return option;
}

//if update is chosen, display the UPDATE menu and return chosen option
int InputHandler::displayUpdateMenu(){
    std::string str;
    int option;
    cout << "1. Update a document" << endl;
    cout << "2. Update a collection" << endl;
    cout << "3. Update a database" << endl;
    getline(cin,str);
    option = stoi(str);
    return option;
}

//if remove is chosen, display the remove menu and return option
int InputHandler::displaySubMenu(){
    std::string temp;
    cout << "Please select an option: " << endl;
    cout << "1. Remove a document" << endl;
    cout << "2. Remove a collection" << endl;
    cout << "3. Remove a database" << endl;
    getline(cin,temp);
    int option = stoi(temp);
    return option;
}
//adds database
void InputHandler::addDB(vector<Database*>* DB){
    string DBname, temp;
    cout << "Enter database name: ";
    getline(cin, DBname);
    Database *databasePtr = new Database(DBname);
    temp = databasePtr->getPath();
    const char *c = temp.c_str();
    if(mkdir(c) == -1){
        cerr << " Error : " << strerror(errno) << endl; //check which error its giving
        cout << "Error in creating database." << endl;
    } else {
        DB->push_back(databasePtr);
        cout << "Database created!" << endl;
    }
}

//adds empty collection to database
void InputHandler::addColl(vector<Database*>* DB){
    string DBchoose, path;
    if (!DB->empty()){
        for (int i = 0; i < DB->size(); i++){
            cout << i << ". " << DB->at(i)->getName() << endl;
        }
        getline(cin, DBchoose);
        cout << "Enter collection path: ";
        getline(cin, path);
        Collection* collPtr = new Collection(path, DB->at(stoi(DBchoose))->getPath());
        DB->at(stoi(DBchoose))->addColl(collPtr);
        ofstream myFile (collPtr->getPath());
        myFile.close();

        //get the new json, and input "[]" in the file
        string temp = collPtr->getPath();
        const char *newPath = temp.c_str();
        char writeBuffer[65536];
        FILE* fp = fopen(newPath, "w");
        Document d;
        FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));
        PrettyWriter<FileWriteStream> writer(os);
        const char* json = "[]";
        d.Parse(json);
        d.Accept(writer);
        fclose(fp);
    }
}

void InputHandler::addDoc(vector<Database*>* DB){
    string input, DBchoose, collChoose, path;
    cout << "Choose a database: " << endl;
    for (int i = 0; i < DB->size(); i++){
        cout << i << ". " << DB->at(i)->getName() << endl;
    }
    getline(cin, DBchoose);
    if (!DB->at(stoi(DBchoose))->getCollections().empty()){
        cout << "Choose a collection: " << endl;
        DB->at(stoi(DBchoose))->print();
        getline(cin, collChoose);
        cout << "Enter document: ";
        getline(cin, input);
        const char* json = input.c_str();
        path = DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->getPath();
        const char *jsonPath = path.c_str();
        Document* d = new Document();
        Document d2;

        //read in existing data from file, push back new json object
        FILE* fp = fopen(jsonPath, "r"); // non-Windows use "r"
        char readBuffer[65536];
        FileReadStream is(fp, readBuffer, sizeof(readBuffer));
        d->ParseStream(is);
        fclose(fp);
        d2.Parse(json);
        Value v(d2, d->GetAllocator());
        d->PushBack(v, d->GetAllocator());

        //rewrite to the file with updated array of json objects
        char writeBuffer[65536];
        FILE* fp2 = fopen(jsonPath, "w");
        FileWriteStream os(fp2, writeBuffer, sizeof(writeBuffer));
        PrettyWriter<FileWriteStream> writer(os);
        d->Accept(writer);
        fclose(fp2);

        //create a doc* and set data to new inputted json obj
        //add this new doc* to our collection
        Document* docPass = new Document();
        docPass->CopyFrom(d2, docPass->GetAllocator());
        DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->addDoc(docPass);
    } else {
        cout << "No collection to choose from. " << endl;
    }
}


//takes in a reference of vector of database*, populates the vector with data
void InputHandler::readData(vector<Database*>* DB){
    string path = "STORAGE\\";
    string DBname, collName;
    string path2, path3;

    auto start1 = high_resolution_clock::now();    
    for (const auto & file : directory_iterator(path)){
        path2 = file.path().string(); // STORAGE\Database1
        DBname = path2.substr(path2.find("\\")+1, path2.length()-path2.find("\\"));
        Database* databasePtr = new Database(DBname);
        DB->push_back(databasePtr);
        for (const auto & file: directory_iterator(path2)){
            path3 = file.path().string(); //STORAGE\Database1\sample1.json
            string temp = path3.substr(path3.find("\\")+1, path3.length()-path3.find("\\")); 
            collName = temp.substr(temp.find("\\")+1, temp.length()-temp.find("\\")); 
            Collection* collPtr = new Collection(collName, DB->back()->getPath());
            DB->back()->addColl(collPtr);
            string temp2 = collPtr->getPath();
            const char *jsonPath = temp2.c_str();
            Document* d = new Document();
            FILE* fp = fopen(jsonPath, "rb");
            char readBuffer[65536];
            FileReadStream is(fp, readBuffer, sizeof(readBuffer));
            d->ParseStream(is);
            fclose(fp);
            
            for (rapidjson::Value::ConstValueIterator itr = d->Begin(); itr != d->End(); ++itr) {
                const rapidjson::Value& attribute = *itr;
                assert(attribute.IsObject()); // each attribute is an object
                Document *t = new Document();
                t->CopyFrom(attribute, t->GetAllocator());
                collPtr->addDoc(t);
            }
        }
    }
    auto stop = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(stop - start1);
    cout << "Time taken to search: "<< duration.count() << " microseconds" << endl;
}

void InputHandler::removeDB(vector<Database*>* DB){
    string DBchoose, temp;
    if (DB->size() != 0){ //if database !empty, display them
        cout << "Choose a database: " << endl;
        for (int i = 0; i < DB->size(); i++){
            cout << i << ". " << DB->at(i)->getName() << endl;
        }
        getline(cin, DBchoose);
        temp = DB->at(stoi(DBchoose))->getPath();
        const char *c = temp.c_str();
        if (!DB->at(stoi(DBchoose))->getCollections().empty()) {
            DB->at(stoi(DBchoose))->deleteFiles();
        }
        if(rmdir(c) == -1){
            //cerr << " Error : " << strerror(errno) << endl;
            cout << "Error in removing database." << endl;
        } else {
            DB->erase(DB->begin() + stoi(DBchoose));
            cout << "Database removed!" << endl;
        }
    } else { //database empty
        cout << "No databases to choose from." << endl;
    }
}

void InputHandler::removeColl(vector<Database*>* DB){
    string DBchoose, collChoose, temp;
    if (DB->size() != 0){ //if database !empty, display them
        cout << "Choose a database: " << endl;
        for (int i = 0; i < DB->size(); i++){
            cout << i << ". " << DB->at(i)->getName() << endl;
        }
        getline(cin, DBchoose);
        cout << "Choose a collection from this database: " << endl;
        DB->at(stoi(DBchoose))->print(); //assuming DBchoose is int, prints all collections in the chosen DB
        getline(cin, collChoose);
        temp = DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->getPath();
        const char *c = temp.c_str();
        if(remove(c) != 0){
            //cerr << " Error : " << strerror(errno) << endl;
            cout << "Error in removing collection." << endl;
        } else {
            DB->at(stoi(DBchoose))->removeColl(stoi(collChoose));
            cout << "Collection removed." << endl;
        }
    } else { //database empty
        cout << "No databases to choose from." << endl;
    }
}

//updates database
void InputHandler::updateDB(vector<Database*>* DB){
    string DBchoose, tempName;
    string tempDB;
    cout << "Choose a database: " << endl;
    for (int i = 0; i < DB->size(); i++){
        cout << i << ". " << DB->at(i)->getName() << endl;
    }
    getline(cin, DBchoose);

    cout << "Rename " << DB->at(stoi(DBchoose))->getName() << " to: " <<endl;
    getline(cin, tempName);

    tempDB = DB->at(stoi(DBchoose))->getPath().c_str();
    
    const char* newName = ("STORAGE\\"+tempName).c_str();
    const char* oldName = tempDB.c_str();
    DB->at(stoi(DBchoose))->setPath(tempName);
    rename(oldName, newName);
    DB->at(stoi(DBchoose))->setName(tempName);
    cout << "Database name updated!" << endl;
}

//updates collection
void InputHandler::updateColl(vector<Database*>* DB){
    string DBchoose, collChoose;
    string tempName, tempDB, tempColl, DBname;
    cout << "Choose a database: " << endl;
    for (int i = 0; i < DB->size(); i++){
        cout << i << ". " << DB->at(i)->getName() << endl;
    }
    getline(cin, DBchoose);

    cout << "Choose a collection from this database: " << endl;
    DB->at(stoi(DBchoose))->print(); //assuming DBchoose is int, prints all collections in the chosen DB
    getline(cin, collChoose);

    cout << "Rename " << DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->getName() << " to: " <<endl;
    getline(cin, tempName);

    //puts selected DB and Coll into a temp string
    tempDB = DB->at(stoi(DBchoose))->getPath().c_str();
    tempColl = DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->getPath().c_str();
    DBname = DB->at(stoi(DBchoose))->getName();

    cout<< DBname <<endl;

    cout << "tempName: " << tempName << endl;

    //renaming
    const char* newName = ("STORAGE\\"+DBname+"\\"+tempName).c_str();
    const char* oldName = tempColl.c_str();
    DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->setPath(DBname+"\\"+tempName);
    rename(oldName, newName);
    DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->setName(tempName);
    cout << "Collection name updated!" << endl;
}

/*
this function searches a chosen DB and collection, able to search for a key value pair(s),
to search for nested objects, objects must be typed exactly,
can query members in an object using '.'

Examples: 
{ "name" : "michael"}
{ "name" : "michael", "age" : 21}
{ "driver" : {"name":"kevin", "age":21}}
{ "driver.age" : 21 }
*/
void InputHandler::searchQuery(vector<Database*>* DB){
    string DBchoose, collChoose, docInput, keyName, objName, attName;
    int count, type, matches=0, results=0;
    cout << "Choose a database: " << endl;
    for (int i = 0; i < DB->size(); i++){
        cout << i << ". " << DB->at(i)->getName() << endl;
    }
    getline(cin, DBchoose);
    if (!DB->at(stoi(DBchoose))->getCollections().empty()){
        cout << "Choose a collection: " << endl;
        DB->at(stoi(DBchoose))->print();
        getline(cin, collChoose);
        cout << "Enter a document. EX: { \"name\" : \"michael\" }" << endl;
        getline(cin, docInput);
        const char *docToC = docInput.c_str();
        Document d, d2;
        d.Parse(docToC);
        count = d.MemberCount(); 
        Collection* coll = new Collection();
        coll = DB->at(stoi(DBchoose))->getCollection(stoi(collChoose));

        for (int i = 0; i < coll->getDocs().size(); i++){
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

    } else {
        cout << "No collections in this database. " << endl;
    }
}

void InputHandler::removeDoc(vector<Database*>* DB){
    string DBchoose, collChoose, docInput;
    int count=0, type, matches=0, results=0;
    cout << "Choose a database: " << endl;
    for (int i = 0; i < DB->size(); i++){
        cout << i << ". " << DB->at(i)->getName() << endl;
    }
    getline(cin, DBchoose);
    if (!DB->at(stoi(DBchoose))->getCollections().empty()){
        cout << "Choose a collection: " << endl;
        DB->at(stoi(DBchoose))->print();
        getline(cin, collChoose);}

    Collection* coll = new Collection();
    coll = DB->at(stoi(DBchoose))->getCollection(stoi(collChoose));
    for (int i = 0; i < coll->getDocs().size(); i++){
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->getDocAt(i)->Accept(writer);
        cout << count << ". " << buffer.GetString() << endl;
        count++;
    }

    // for (int j = 0; j < DB->at(stoi(DBchoose))->collections.at(i)->getDocs().size(); j++){
    // StringBuffer buffer;
    // Writer<StringBuffer> writer(buffer);
    // this->collections.at(i)->getDocAt(j)->Accept(writer);
    // cout << "\t" << buffer.GetString() << endl;
}



/*threads functions so that we can read multiple databases/collections concurrently
    dynamically create threads based on database size + collection size
*/

void InputHandler::threadRead(vector<Database*>* DB){
    string path = "STORAGE\\";
    int count = 0;
    bool begin;
    begin = DB->empty();
    bool begin2 = DBPaths.empty();
    //Dynamically creates 1 thread per DB

    begin2 = true;
    //cout << "DBpath size: " << DBPaths.size() << endl;
    //cout << "DB size: " << DB->size() << endl;
    if (begin == true){
        auto start1 = high_resolution_clock::now();    
        for (int i = 0; i<DBPaths.size(); i++)
        {
            //std::cout << DBPaths[i] << endl;
            DB_threads.push_back(thread(&InputHandler::singleRead, this, cref(DB), ref(DBPaths[i])));
            // if (begin2 == true){
            // for (int i=0; i<CollPaths.size(); i++){
            //     //std::cout << CollPaths[i]<<endl;
            //     Coll_threads.push_back(thread(&InputHandler::singleCollRead, this, cref(DB), cref(DBPaths[i]), cref(CollPaths[i])));
            //     count++;
            // }
            // }
            // else if (begin2 == false){
            //     count++;
            //     //cout << "Colls Read" << endl;
            // }
        }
        for (auto& t: DB_threads){
            t.join();
        }
        // for(auto& th: Coll_threads){
        //     th.join();
        // }
        auto stop = high_resolution_clock::now();
        auto duration = duration_cast<microseconds>(stop - start1);
        cout << "Time taken to read: "<< duration.count() << " microseconds" << endl;
        begin = false;
        begin2 = false;
        }
        else{
        cout << "Already read" << endl;
     }
}


void InputHandler::fillPaths(){
    string path = "STORAGE\\";
    string DBname, collName;
    string path2, path3;
    //Find DB
    for (const auto & file : directory_iterator(path)){
        path2 = file.path().string(); // STORAGE\'FirstDatabaseFound'
        DBname = path2.substr(path2.find("\\") + 1, path2.length()-path2.find("\\"));

        Database* databasePtr = new Database(DBname);
        DBPaths.push_back(DBname);

        for (const auto & file: directory_iterator(path2)){
            path3 = file.path().string(); //STORAGE\Database1\sample1.json
            string temp = path3.substr(path3.find("\\")+1, path3.length()-path3.find("\\")); 
            collName = temp.substr(temp.find("\\")+1, temp.length()-temp.find("\\")); 
            //Collection* collPtr = new Collection(collName, DB->back()->getPath());
            //DB->back()->addColl(collPtr);
            //string temp2 = collPtr->getPath();
            CollPaths.push_back(collName);
            // const char *jsonPath = temp2.c_str();
            // Document* d = new Document();
            // FILE* fp = fopen(jsonPath, "rb");
            // char readBuffer[65536];
            // FileReadStream is(fp, readBuffer, sizeof(readBuffer));
            // d->ParseStream(is);
            // fclose(fp);
            
            // for (rapidjson::Value::ConstValueIterator itr = d->Begin(); itr != d->End(); ++itr) {
            //     const rapidjson::Value& attribute = *itr;
            //     assert(attribute.IsObject()); // each attribute is an object
            //     Document *t = new Document();
            //     t->CopyFrom(attribute, t->GetAllocator());
            //     collPtr->addDoc(t);
            // }
        }
    }

    // for (int i = 0;i<CollPaths.size(); i++)
    // {
    //     cout << CollPaths[i] << endl;
    // }
}


//reads single Coll (to be used by thread)
mutex mtx;
void InputHandler::singleCollRead(vector<Database*>* DB, string DBpath, string Collpath){
    lock_guard<mutex> lock(mtx);
    string path = "STORAGE\\";
    string DBname, collName;
    string path2, path3;

    path2 = path + DBpath; // STORAGE\DB1
    DBname = path2.substr(path2.find("\\") + 1, path2.length()-path2.find("\\"));

    //cout << "collread: " << DBname << endl;

    path3 = path2 + Collpath; //STORAGE\Database1\sample1.json
    string temp = path3.substr(path3.find("\\")+1, path3.length()-path3.find("\\")); 
    //cout << "path3: " << path3 << endl;

    collName = temp.substr(temp.find("\\")+1, temp.length()-temp.find("\\")); 
    Collection* collPtr = new Collection(collName, DB->back()->getPath());
    //cout << "Path3: " << path3 << endl;
    DB->back()->addColl(collPtr);
    string temp2 = collPtr->getPath();
    const char *jsonPath = temp2.c_str();
    Document* d = new Document();
    FILE* fp = fopen(jsonPath, "rb");
    char readBuffer[65536];
    FileReadStream is(fp, readBuffer, sizeof(readBuffer));
    d->ParseStream(is);
    fclose(fp);

    for (rapidjson::Value::ConstValueIterator itr = d->Begin(); itr != d->End(); ++itr) {
    const rapidjson::Value& attribute = *itr;
    assert(attribute.IsObject()); // each attribute is an object
    Document *t = new Document();
    t->CopyFrom(attribute, t->GetAllocator());
    collPtr->addDoc(t);
    }
}


mutex mtx2;
void InputHandler::singleRead(vector<Database*>* DB, string pathname){
    lock_guard<mutex> lock(mtx2);
    string path = "STORAGE\\";
    string DBname, collName, temp;
    string path2, path3;

    //Find DBs and read
    path2 = path + pathname;
    //cout << "name:" << path2 << endl;
    DBname = path2.substr(path2.find("\\") + 1, path2.length()-path2.find("\\"));
    Database* databasePtr = new Database(DBname);
    DB->push_back(databasePtr);

    //Find Colls and read
    for (const auto & file: directory_iterator(path2)){
    path3 = file.path().string(); //STORAGE\Database1\sample1.json
    string temp = path3.substr(path3.find("\\")+1, path3.length()-path3.find("\\")); 
    collName = temp.substr(temp.find("\\")+1, temp.length()-temp.find("\\")); 
    Collection* collPtr = new Collection(collName, DB->back()->getPath());
    //cout << "Path3: " << path3 << endl;
    DB->back()->addColl(collPtr);
    string temp2 = collPtr->getPath();
    const char *jsonPath = temp2.c_str();
    Document* d = new Document();
    FILE* fp = fopen(jsonPath, "rb");
    char readBuffer[65536];
    FileReadStream is(fp, readBuffer, sizeof(readBuffer));
    d->ParseStream(is);
    fclose(fp);

    for (rapidjson::Value::ConstValueIterator itr = d->Begin(); itr != d->End(); ++itr) {
        const rapidjson::Value& attribute = *itr;
        assert(attribute.IsObject()); // each attribute is an object
        Document *t = new Document();
        t->CopyFrom(attribute, t->GetAllocator());
        collPtr->addDoc(t);
        }
    }
}