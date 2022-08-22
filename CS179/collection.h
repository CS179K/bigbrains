#pragma once

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/filewritestream.h"
#include <iostream>
#include <string>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <fstream>
#include <filesystem>

using namespace std;
using namespace rapidjson;

class Collection{
private:
    const std::string folder = "STORAGE/";
    string name;
    string path;
    vector<Document*> documents;
public:
    Collection();
    Collection(string fileName, string dbPath) {this->path = dbPath+fileName; this->name = fileName;}
    string getPath(){return this->path;}
    string getName(){return this->name;}
    void addDoc(Document*);
    Document* getDocAt(int);
    vector<Document*> getDocs() {return this->documents;}
    void setName(std::string);
    void setPath(std::string n) {this->path = folder+n;}
    int getNumOfDocuments(); 
};


Collection::Collection(){}

//set the name of this collection
void Collection::setName(std::string n){
    this->name = n;
    //cout << "in setName: " << endl << this->name << endl;
}

// takes in a document* and adds it to vector
void Collection::addDoc(Document* d){
    this->documents.push_back(d);
}

// takes in an index, returns the Document* at the index
Document* Collection::getDocAt(int i){
    return this->documents.at(i);
}
int Collection::getNumOfDocuments(){
  int size = this->documents.size(); 
  return size;  
}