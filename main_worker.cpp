#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdlib>
#include <string>
#include  <sys/types.h>
#include  <dirent.h>
#include  <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include  "dateToInt.h"
#include "BucketNode.h"
#include "methods.h"
#include "inbetweenDates.h"
#include "BST.h"
#include "CountriesList.h"
#include "DatesList.h"
#include "ChildParameters.h"
using namespace std;

void catchinterrupt_worker(int signo) { // SIGINT+SIGQUIT
    printf("\nCatching SIGINT: signo=%d\n", signo);
    writeLogWorker();
    cout << "Bye Worker." << endl;
    exit(0);
}

void catchinterrupt_refresh_worker(int signo) { // SIGUSR1
    printf("\nCatching SIGINT: signo=%d\n", signo);
    
        int newFiles=0;
        ChildParameters* chpar;
        chpar=getChildParams();
        
      //  int r_fd = open((*chpar).s_name.c_str(), O_RDONLY);
      //  int s_fd = open((*chpar).r_name.c_str(), O_WRONLY);
    
        string inputdir = (*chpar).inputdir;
    
//        char * packet = new char[(*chpar).bufferSize];
        string path;
        DatesListNode* found;
        
        CountriesLinkedList* countries;
        countries=getCountriesL();
        
    
        CountriesNode* current = countries->head;
        while (current != NULL) {

            DIR *dir_ptr;
            struct dirent *direntp;

            if ((dir_ptr = opendir(current->rec->filesystem_path.c_str())) == NULL) {
                fprintf(stderr, "cannot open %s \n", current->rec->filesystem_path.c_str());
               

            } else {
                while ((direntp = readdir(dir_ptr)) != NULL) {
                    if (*direntp->d_name != '.') {//an to date den einai '.'
                        found=current->rec->dates.searchList(direntp->d_name);
                        
                        if(found==NULL){ //an de vrethike simainei oti einai neo date
                            cout<<direntp->d_name<<endl;
                            current->rec->dates.insertNode(direntp->d_name);
                            path = current->rec->filesystem_path + "/" + direntp->d_name;
                            ifstream recordsfile(path.c_str()); //anoigei to arxeio gia ta records
                            
                            if (recordsfile.is_open() && recordsfile.good()) {
                                string line = "";
                                int l = 0;
                                while (getline(recordsfile, line)) {
                                    stringstream command(line);
                                    string parameter;
                                    string names[8] = {"ID", "firstname", "lastname", "disease", "country", "entrydate", "exitdate", "age"};
                                    string data[8] = {"", "", "", "", "", "", ""};
                                    command >> data[0] >> parameter >> data[1] >> data[2] >> data[3] >> data[7]; // >> data[5] >> data[6] >> data[7];

                                    data[4] = current->rec->name;
                                    data[5] = direntp->d_name;
                                    data[6] = "-";

                                    for (int i = 0; i < 8; i++) {
                                        if (data[i] == "") {
                
                                            cout << names[i] << " is missing in line " << l << endl;
                                            cout << "cannot proceed with incorrect file" << endl;
                                      
                                        }
                                    }

                                    Record * record = new Record(data);

                                    if (parameter == "ENTER") {
                                  
                                        insertPatientRecord(false, record); 
                                        
                                    } else {
                                        recordPatientExit(false, data[0], direntp->d_name);
                                        
                                    }
                                    l++;
                                }
                                workerTotal();
                            } else {
                                cout << "Failed to open file..";
                              
                            }
                
                            newFiles++;
                        }

                    }
                }

                current->rec->dates.sort();
                closedir(dir_ptr);
            }

            current = current->next;
        }
        cout<<newFiles<<" new files detected and added"<<endl;
   exit(0);
}

int main_worker(ChildParameters parameters) {

    int h1 = 500, h2 = 500, b = 3000;
    workerTotal();
    int bucketsize = (b - sizeof (BucketNode*) - sizeof (Data*)) / (sizeof (Data));
    bool messages = true;
    setChildParams(&parameters);cout<<"cc"<<endl;
    printf("b = %d \n", b);
    printf("h1 = %d \n", h1);
    printf("h2 = %d \n", h2);
    printf("bucketsize = %d \n", bucketsize);

    int r_fd = open(parameters.s_name.c_str(), O_RDONLY);
    int s_fd = open(parameters.r_name.c_str(), O_WRONLY);

    string inputdir = parameters.inputdir;

    CountriesLinkedList * countries = new CountriesLinkedList();
 
    //diavazei tis xwres pou tou anatethikan
    char * packet = new char[parameters.bufferSize];

    while (true) {
        read(r_fd, packet, parameters.bufferSize);

        if (packet[0] == '-') {
            break;
        } else {
            string name = packet;
            countries->insertNode(new Country(name, inputdir + "/" + name));
            
        }
    }
    delete [] packet;

    countries->printListDetails();
    setCountriesL(countries);//aplws setter 
    
    initializeHashtables(h1, h2, bucketsize); //arxikopoiisi

    CountriesNode* current = countries->head;
    while (current != NULL) {

        DIR *dir_ptr;
        struct dirent *direntp;

        if ((dir_ptr = opendir(current->rec->filesystem_path.c_str())) == NULL) {
            fprintf(stderr, "cannot open %s \n", current->rec->filesystem_path.c_str());
            
            workerErrors();
            workerTotal();
            return -1;
        } else {
            workerTotal();
            while ((direntp = readdir(dir_ptr)) != NULL) {
                if (*direntp->d_name != '.') {
                    current->rec->dates.insertNode(direntp->d_name);
                   
                }//vazei ola ta dates
            }//se kathe xwra

            current->rec->dates.sort();
            closedir(dir_ptr);
        }

        current = current->next;
    }

    current = countries->head;
    while (current != NULL) {
        cout << "Country: " << current->rec->name << " has the following dates: " << endl;

        DatesListNode* currentDate = current->rec->dates.head;
        while (currentDate != NULL) {
            cout << " - " << currentDate->date << endl;


            string path = current->rec->filesystem_path + "/" + currentDate->date;

            ifstream recordsfile(path.c_str()); //anoigei to arxeio gia na parei ta records

            if (recordsfile.is_open() && recordsfile.good()) {
                string line = "";
                int l = 0;
                while (getline(recordsfile, line)) {
                    stringstream command(line);
                    string parameter;
                    string names[8] = {"ID", "firstname", "lastname", "disease", "country", "entrydate", "exitdate", "age"};
                    string data[8] = {"", "", "", "", "", "", ""};
                    command >> data[0] >> parameter >> data[1] >> data[2] >> data[3] >> data[7]; // >> data[5] >> data[6] >> data[7];

                    data[4] = current->rec->name;
                    data[5] = currentDate->date;
                    data[6] = "-";

                    for (int i = 0; i < 8; i++) {
                        if (data[i] == "") {
                            workerTotal();
                            workerErrors();
                            cout << names[i] << " is missing in line " << l << endl;
                            cout << "cannot proceed with incorrect file" << endl;
                           
                            return 0;
                        }
                    }

                    Record * record = new Record(data);

                    if (parameter == "ENTER") {
                       
                        insertPatientRecord(false, record); //eisagwgi twn records apo to arxeio
                       
                    } else {
                        recordPatientExit(false, data[0], currentDate->date);
                       
                    }
                    l++;
                }
                workerTotal();
            } else {
                cout << "Failed to open file..";
             
                workerTotal();
                workerErrors();
            }

            
//            OutputParameters output(r_fd, s_fd, parameters.bufferSize);
//            summaryStatistics(currentDate->date,current->rec->name,output);

            
            currentDate = currentDate->next;
        }

        current = current->next;
    }



    static struct sigaction act;
    act.sa_handler = catchinterrupt_worker;
    sigfillset(&(act.sa_mask));
    sigaction(SIGINT, &act, NULL);
    sigaction(SIGQUIT, &act, NULL);

    static struct sigaction act2;
    act2.sa_handler = catchinterrupt_worker;
    sigfillset(&(act.sa_mask));
    sigaction(SIGUSR1, &act2, NULL);


    string line = "";


    bool error;

    packet = new char[parameters.bufferSize];
    
    
//dexetai tis entoles
    while (true) {
        error = false;

        if (read(r_fd, packet, parameters.bufferSize) <= 0) {
            break;
        }

        line = packet;

        if (line != "") { //dexetai entoles
            stringstream command(line);
            string entoli;

            string names[8] = {"ID", "firstname", "lastname", "disease", "country", "entrydate", "exitdate"};
            string data[8] = {"", "", "", "", "", "", "", ""};

            command >> data[0] >> data[1] >> data[2] >> data[3] >> data[4] >> data[5] >> data[6] >> data[7];

            if (data[0] == "listCountries" || data[0] == "/listCountries") {
                countries->printListDetails();
                workerTotal();
                

            }
            else if (data[0] == "insertPatientRecord" || data[0] == "/insertPatientRecord") {
                for (int i = 1; i < 7; i++) {
                    if (data[i] == "") {
                        cout << names[i] << " is missing " << endl;
                        cout << "cannot proceed with incorrect file" << endl;
                        error = true;
                        workerTotal();
                        workerErrors();
                    }
                }
                if (!error) {
                    if (data[7] == "") {
                        data[7] = "-";
                    }
                    string data2[7] = {data[1], data[2], data[3], data[4], data[5], data[6], data[7]};
                    Record * record = new Record(data2);
                    insertPatientRecord(messages, record);
                    workerTotal();
                }
            } else if (data[0] == "/globalDiseaseStats" || data[0] == "globalDiseaseStats") {
                if ((data[1] == "" && data[2] != "") || (data[1] != "" && data[2] == "")) {
                    cout << "error: You have to enter either two dates or no dates at all" << endl;
                    workerTotal();
                    workerErrors();
                  
                } else {
                    workerTotal();
                  
                    if (data[1] == "") {
                        globalDiseaseStats(messages, NULL, NULL);
                    } else {
                        globalDiseaseStats(messages, &data[1], &data[2]);
                    }
                }
            } else if (data[0] == "/searchPatientRecord" || data[0] == "searchPatientRecord") {
                if (data[1] == "") {
                    workerTotal();
                    workerErrors();
                    cout << "error: You have to enter a record ID" << endl;
                   
                } else {//me select
                    bool found;
                    OutputParameters output(r_fd, s_fd, parameters.bufferSize);
                    searchPatientRecord(messages, data[1], output,&found);
                    workerTotal();
                    
                    
                    //xwris select 
//                    OutputParameters output(r_fd, s_fd, parameters.bufferSize);
//                    bool found=false;
//                    searchPatientRecord(messages, data[1], output,&found);
//
//                    char * packet = new char[parameters.bufferSize];
//                    if(found)
//                        strcpy(packet, "###");
//                    else
//                        strcpy(packet, "-");
//                    write(s_fd, packet, parameters.bufferSize);
//                    delete [] packet;

                }
            } else if (data[0] == "/diseaseFrequency" || data[0] == "diseaseFrequency") {
                if (data[1] == "" || data[3] == "" || data[2] == "") {
                    workerTotal();
                    workerErrors();
                    cout << "error: you have to enter one virus name,(maybe a country-optional) and two dates" << endl;
                    
                } else {
                   
                    workerTotal();
                    if (data[4] == "") {//select
                        OutputParameters output(r_fd, s_fd, parameters.bufferSize);
                        diseaseFrequency(messages, data[1], NULL, data[2], data[3], output);
                        
                       
                    } else {
                        //gia sygkekrimeno country
                        OutputParameters output(r_fd, s_fd, parameters.bufferSize);
                        diseaseFrequency(messages, data[1], &data[4], data[3], data[2], output);

                        char * packet = new char[parameters.bufferSize];
                        strcpy(packet, "-");
                        write(s_fd, packet, parameters.bufferSize);
                        delete [] packet;
                    }
                }

            } else if (data[0] == "/topk-AgeRanges" || data[0] == "topk-AgeRanges") {
                if (data[1] == "" || data[2] == "" || (data[3] == "" && data[4] != "") || (data[3] != "" && data[4] == "")) {
                    cout << "error: you have to enter a number(top k), a country ,a disease and two dates" << endl;
                    workerTotal();
                    workerErrors();
                   
                } else {
                    workerTotal();
                   
                    OutputParameters output(r_fd, s_fd, parameters.bufferSize);
                    topk_AgeRanges(messages, atoi(data[1].c_str()), data[2], data[3], data[4], data[5], output);

                    char * packet = new char[parameters.bufferSize];
                    strcpy(packet, "-");
                    write(s_fd, packet, parameters.bufferSize);
                    delete [] packet;

                }
            } else if (data[0] == "/numPatientAdmissions" || data[0] == "numPatientAdmissions") {
                if (data[1] == "" || data[2] == "" || data[3] == "") {
                    workerTotal();
                    workerErrors();
                  
                    cout << "error: you have to enter one virus name,two dates and (maybe a country-optional)" << endl;
                } else {
                    workerTotal();
                    if(data[4]=="")
                    {//select
                        OutputParameters output(r_fd, s_fd, parameters.bufferSize);
                        numPatientAdmissions(messages, data[1], data[2], data[3], NULL, output);
                        
                        
                        //                    if (data[4] == "") {//an de dothei country 
                        //                        CountriesNode* current;
                        //                        current = countries->head;
                        //                        while (current != NULL) {//an vrethei i astheneia
                        //                       //     numPatientAdmissions(messages, data[1], data[2], data[3], current->rec->name);
                        //                            current = current->next;
                        //                        }
                        
                    } else {
                        //gia sygkekrimeno country
                        OutputParameters output(r_fd, s_fd, parameters.bufferSize);
                        numPatientAdmissions(messages, data[1], data[2], data[3], &data[4], output);
                        char * packet = new char[parameters.bufferSize];
                        strcpy(packet, "-");
                        write(s_fd, packet, parameters.bufferSize);
                        delete [] packet;

                }
                }
            } else if (data[0] == "/numPatientDischarges" || data[0] == "numPatientDischarges") {
                if (data[1] == "" || data[2] == "" || data[3] == "") {
                    workerTotal();
                    workerErrors();
                    
                    cout << "error: you have to enter one virus name,two dates and (maybe a country-optional)" << endl;
                } else {
                    workerTotal();
                    if(data[4]==""){
                        OutputParameters output(r_fd, s_fd, parameters.bufferSize);
                        numPatientDischarges(messages, data[1], data[2], data[3], NULL, output);
                    }
                   
                    //                    if (data[4] == "") {//an de dothei country 
                    //                        CountriesNode* current;
                    //                        current = countries->head;
                    //                        while (current != NULL) {//an vrethei i astheneia
                    //                            numPatientDischarges(messages, data[1], data[2], data[3], current->rec->name);
                    //                            current = current->next;
                    //                        }
                    else {
                    OutputParameters output(r_fd, s_fd, parameters.bufferSize);
                    numPatientDischarges(messages, data[1], data[2], data[3],&data[4], output);
                    char * packet = new char[parameters.bufferSize];
                    strcpy(packet, "-");
                    write(s_fd, packet, parameters.bufferSize);
                    delete [] packet;

                    }

                }

            } else if (data[0] == "/recordPatientExit" || data[0] == "recordPatientExit") {
                if (data[1] == "" || data[2] == "") {
                 
                    cout << "error:you have to enter recordId and exitdate" << endl;
                    workerTotal();
                    workerErrors();
                } else {
                    workerTotal();
                   
                    recordPatientExit(messages, data[1], data[2]);
                }
            } else if (data[0] == "/numCurrentPatients" || data[0] == "numCurrentPatients") {
                if (data[1] == "") {
                    workerTotal();
                    workerErrors();
                    numCurrentPatients(messages, NULL);
                } else {
                    workerTotal();
                   
                    numCurrentPatients(messages, &data[1]);
                }
//            }else if (data[0] == "summaryStatistics") {
//                    workerTotal();
//                    workerErrors();
//                        OutputParameters output(r_fd, s_fd, parameters.bufferSize);
//                        summaryStatistics();
//                }
            }else {
                cout << "Invalid Command" << endl;
                workerTotal();
                workerErrors();
                //    structures.errors++;
                cout << "You can use one of the following:" << endl;
                cout << "exit" << endl;
                //cout << "insertPatientRecord" << endl;
               // cout << "recordPatientExit" << endl;
                //cout << "numCurrentPatients" << endl;
               //cout << "globalDiseaseStats" << endl;
                cout << "diseaseFrequency" << endl;
                cout << "topk-AgeRanges" << endl;
                cout << "numPatientAdmissions" << endl;
                cout << "numPatientDischarges" << endl;
                cout << "listCountries" << endl;
                cout << "searchPatientRecord" << endl;

            }
        } else { //an vrei eof
            workerTotal();
            cleanupStructures();
            break;
        }

    }

    delete [] packet;


    close(s_fd);
    close(r_fd);


    return 0;
}
