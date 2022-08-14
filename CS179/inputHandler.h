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
#include <sstream>
#include <string>
#include <filesystem>
#include <vector>
#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include "database.h"

using namespace std;
using std::filesystem::directory_iterator;
using namespace rapidjson;

class InputHandler{
public:
    int displayMenu();
    int displayAddMenu();
    int displaySubMenu();
    int displayUpdateMenu();
    int displaySearchMenu();
    void addDB(vector<Database*>*);
    void addColl(vector<Database*>*);
    void addDoc(vector<Database*>*);
    void readData(vector<Database*>*);
    void removeDB(vector<Database*>*);
    void removeColl(vector<Database*>*);
    void updateDB(vector<Database*>*);
    void updateColl(vector<Database*>*);
    void searchQuery(vector<Database*>*);
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
    cout << "5. Print" << endl;
    cout << "6. Quit" << endl;
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
    string DBchoose, path, temp;
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
        Document* d = new Document();
        d->Parse(json);
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        d->Accept(writer);
        if (buffer.GetString() != NULL){
            path = DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->getPath();
            const char *c = path.c_str();
            FILE* fp = fopen(c, "a");
            char writeBuffer[65536];
            FileWriteStream os(fp, writeBuffer, sizeof(writeBuffer));
            Writer<FileWriteStream> writer(os);
            d->Accept(writer);
            DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->addDoc(d);
            fclose(fp);
        } else {
            cout << "Error in input. No additions made." << endl;
        }
    } else {
        cout << "No collection to choose from. " << endl;
    }
}

void InputHandler::readData(vector<Database*>* DB){
    string path = "STORAGE\\";
    string DBname, collName;
    string path2, path3;
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
            collPtr->addDoc(d);
        }
    }
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
    //cout << "new name: " << newName << endl;
    //cout << "old name: " << oldName << endl;
    rename(oldName, newName);
    DB->at(stoi(DBchoose))->setName(tempName);
    cout << "Database name updated!" << endl;

    //cout << DB->at(stoi(DBchoose))->getName() << endl;
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
    //cout << endl << DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->getPath()<< endl;
    //cout << "new name: " << newName << endl;
    //cout << "old name: " << oldName << endl;
    rename(oldName, newName);
    DB->at(stoi(DBchoose))->getCollection(stoi(collChoose))->setName(tempName);
    cout << "Collection name updated!" << endl;

    //cout << DB->at(stoi(DBchoose))->getName() << endl;
}

void InputHandler::searchQuery(vector<Database*>* DB){
    string DBchoose, collChoose, docInput;
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
            matches = 0;     
            d2.CopyFrom(*coll->getDocAt(i), d2.GetAllocator()); //d2 copies current document* data
            for (Value::ConstMemberIterator iter = d.MemberBegin(); iter != d.MemberEnd(); ++iter){
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
                            }
                            case 3:{
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