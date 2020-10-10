#include <iostream>
#include <sstream>
#include <fstream>
#include <cstdlib>
#include <string>
#include  <sys/types.h>
#include  <dirent.h>
#include <signal.h>
#include <unistd.h>
#include <cstring>
#include "BucketNode.h"
#include "inbetweenDates.h"
#include "BST.h"
#include "CountriesList.h"
#include "methods_aggregator.h"

using namespace std;

void catchinterrupt_aggregator(int signo) { 
    printf("\nCatching SIGINT: signo=%d\n", signo);

    cleanupWorkers();
    cleanupPipes();
    writeLog();

    cout << "Bye Aggregator." << endl;
    exit(0);
}

int main(int argc, char * argv[]) {
    int w, bufferSize;
    string inputdir;
    aggrTotal();

    if (argc != 7) {
        aggrErrors();
        aggrTotal();
        printf("lathos parametroi \n");
        printf("prepei na einai: -p patientRecordsFile –h1 diseaseHashtableNumOfEntries –h2 countryHashtableNumOfEntries –b bucketSize \n");
        return 0;
    }
    //elegxos gia orismata
    if (string(argv[1]) == "-w") {
        aggrTotal();
        w = atoi(argv[2]);
    }//h thesh tou h1
    else if (string(argv[3]) == "-w") {
        aggrTotal();
        w = atoi(argv[4]);
    } else if (string(argv[5]) == "-w") {
        aggrTotal();
        w = atoi(argv[6]);
    } else {
        aggrErrors();
        aggrTotal();
        printf("lathos parametroi \n");
        printf("prepei na einai: -i inputdir –w numWorkers  –b bucketSize \n");
        return 0;
    }
    if (string(argv[1]) == "-b") {
        aggrTotal();
        bufferSize = atoi(argv[2]);
    } else if (string(argv[3]) == "-b") {
        aggrTotal();
        bufferSize = atoi(argv[4]);
    } else if (string(argv[5]) == "-b") {
        aggrTotal();
        bufferSize = atoi(argv[6]);
    } else {
        aggrErrors();
        aggrTotal();
        printf("lathos parametroi \n");
        printf("prepei na einai: -i inputdir –w numWorkers  –b bucketSize \n");
        return 0;
    }
    if (string(argv[1]) == "-i") {
        aggrTotal();
        inputdir = (argv[2]);
    } else if (string(argv[3]) == "-i") {
        aggrTotal();
        inputdir = (argv[4]);
    } else if (string(argv[5]) == "-i") {
        aggrTotal();
        inputdir = (argv[6]);
    } else {
        printf("lathos parametroi \n");
        aggrErrors();
        aggrTotal();
        printf("prepei na einai: -i inputdir –w numWorkers  –b bucketSize \n");
        return 0;
    }

    printf("b = %d \n", bufferSize);
    printf("w = %d \n", w);
    printf("inputdir = %s \n", inputdir.c_str());

    initializeCountries(inputdir); //arxikopoiei tis xwres k tis vazei se lista
    
    initializePipes(w); //arxikopoiisi twn pipes

    initializeWorkers(bufferSize);
  
    assignCountries();//moirazei xwres stous workers

    sendCountries();
    //collectStatisticsWithSelect();
    static struct sigaction act; 
    act.sa_handler = catchinterrupt_aggregator; 
    sigfillset(&(act.sa_mask));
    sigaction(SIGINT, &act, NULL); 
    sigaction(SIGQUIT, &act, NULL);
    
    
    string line = "";

    
    while (true) {
        cout << "Aggregator (pid: " << getpid() << ") dose entoli: ";

        if (getline(cin, line)) { //dexetai entoles
            if (line == "exit" || line == "/exit") {
                cout << "exiting" << endl;
                aggrTotal();
                cleanupWorkers();
                cleanupPipes();
                writeLog();
                break;
            }

            stringstream command(line);
            string entoli;

            string data[8] = {"", "", "", "", "", "", "", ""};

            command >> data[0] >> data[1] >> data[2] >> data[3] >> data[4] >> data[5] >> data[6] >> data[7];

            if (data[0] == "listCountries" || data[0] == "/listCountries") {
                listCountriesForAggregator();
                aggrTotal();
            }else if (data[0] == "/diseaseFrequency" || data[0] == "diseaseFrequency") {
                if (data[1] == "" || data[3] == "" || data[2] == "") {
                    cout << "error: you have to enter one virus name,two dates (and maybe a country-optional) " << endl;
                    aggrErrors();
                    aggrTotal();
                } else {
                    aggrTotal();
                    if (data[4] == "") {
                        // select
                        cout << "Broadcasting to all: " << line << endl;
                        broadcast(line);
                        
                        collectDiseaseFrequencyWithSelect();

                        // seiriaka xwris select 
                        //                        CountriesLinkedList * countries;
                        //                        countries=getCountriesAgg();
                        //                        CountriesNode* currentC;
                        //                        currentC=countries->head;
                        //                        int * fds = findPipeForCountry(currentC->rec->name);
                        //                        char * packet = new char[bufferSize]();
                        //                        string tempS;
                        //                        while(currentC!=NULL){                        
                        //                            cout << "Aggregator, com will use: " << fds[0] << " and " << fds[1] << endl;
                        //
                        //
                        //                            fds = findPipeForCountry(currentC->rec->name);
                        //                            tempS=line+" "+currentC->rec->name;
                        //                            strcpy(packet, tempS.c_str());
                        //
                        //                            write(fds[0], packet, bufferSize);
                        //
                        //                            while (1) {
                        //                                if (read(fds[1], packet, bufferSize) <= 0) { //pairnei tin apantisi
                        //                                    break;
                        //                                }                                                
                        //                                if (strcmp(packet, "-") == 0) {
                        //                                    break;
                        //                                } else {
                        //                                    cout << currentC->rec->name<<" "<<packet << endl;
                        //                                }
                        //
                        //                            }
                        //
                        //                            currentC=currentC->next;
                        //                        }delete [] packet;

                    }
                    else {
                        aggrTotal();
                        int * fds = findPipeForCountry(data[4]);

                        cout << "Aggregator, com will use: " << fds[0] << " and " << fds[1] << endl;

                        char * packet = new char[bufferSize];

                        strcpy(packet, line.c_str());

                        write(fds[0], packet, bufferSize);

                        while (1) {
                            if (read(fds[1], packet, bufferSize) <= 0) { //pairnei tin apantisi
                                break;
                            }
                            if (strcmp(packet, "-") == 0) {
                                break;
                            } else {
                                cout << packet << endl;
                            }

                        }

                        delete [] packet;
                    }
                }               }
             else if (data[0] == "/topk-AgeRanges" || data[0] == "topk-AgeRanges") {
                if (data[1] == "" || data[2] == "" || (data[3] == "" && data[4] != "") || (data[3] != "" && data[4] == "")) {
                    aggrErrors();
                    aggrTotal();
                    cout << "error: you have to enter a number(top k), a country ,a disease and two dates" << endl;
                } else {
                    aggrTotal();
                    int * fds = findPipeForCountry(data[2]);

                    cout << "Aggregator, com will use: " << fds[0] << " and " << fds[1] << endl;

                    char * packet = new char[bufferSize];

                    strcpy(packet, line.c_str());

                    write(fds[0], packet, bufferSize);

                    while (1) {
                        if (read(fds[1], packet, bufferSize) <= 0) { //pairnei tin apantisi
                            aggrErrors();
                            aggrTotal();
                            break;
                        }
                        if (strcmp(packet, "-") == 0) {
                            break;
                        } else {
                            cout << packet << endl;
                        }

                    }

                    delete [] packet;


                }}
             else if (data[0] == "/numPatientAdmissions" || data[0] == "numPatientAdmissions") {
                if (data[1] == "" || data[3] == "" || data[2] == "") {
                    cout << "error: you have to enter one virus name,(maybe a country-optional) and two dates" << endl;
                    aggrErrors();
                    aggrTotal();
                } else {
                    aggrTotal();
                    if (data[4] == "") {
                        
                        cout << "Broadcasting to all: " << line << endl;
                        broadcast(line);
                        
                        collectNumPatientWithSelect();
                        //seiriaka xwris select
//                        CountriesLinkedList * countries;
//                        countries = getCountriesAgg();
//                        CountriesNode* currentC;
//                        currentC = countries->head;
//                        int * fds = findPipeForCountry(currentC->rec->name);
//                        char * packet = new char[bufferSize];
//                        string tempS;
//                        while (currentC != NULL) {
//                            cout << "Aggregator, com will use: " << fds[0] << " and " << fds[1] << endl;
//
//
//                            fds = findPipeForCountry(currentC->rec->name);
//                            tempS = line + " " + currentC->rec->name;
//                            strcpy(packet, tempS.c_str());
//
//                            write(fds[0], packet, bufferSize);
//
//                            while (1) {
//                                if (read(fds[1], packet, bufferSize) <= 0) { //pairnei tin apantisi
//                                    aggrErrors();
//                                    aggrTotal();
//                                    break;
//                                }
//                                if (strcmp(packet, "-") == 0) {
//                                    break;
//                                } else {
//                                    cout << packet << endl;
//                                }
//
//                            }
//
//                            currentC = currentC->next;
//                        }
//                        delete [] packet;
                    } else {
                        int * fds = findPipeForCountry(data[4]);

                        cout << "Aggregator, com will use: " << fds[0] << " and " << fds[1] << endl;

                        char * packet = new char[bufferSize];

                        strcpy(packet, line.c_str());

                        write(fds[0], packet, bufferSize);

                        while (1) {
                            if (read(fds[1], packet, bufferSize) <= 0) { //pairnei tin apantisi
                                aggrErrors();
                                aggrTotal();
                                break;
                                
                            }
                            if (strcmp(packet, "-") == 0) {
                                break;
                            } else {
                                cout << packet << endl;
                            }

                        }

                        delete [] packet;
                    }

                }}
             else if (data[0] == "/searchPatientRecord" || data[0] == "searchPatientRecord") {
                if (data[1] == "") {
                    cout << "error: you have to enter one patient id" << endl;
                    aggrErrors();
                    aggrTotal();
                } else {
                    aggrTotal();
                    cout << "Broadcasting to all: " << line << endl;
                    broadcast(line);
                        
                    collectSearchRecordIdWithSelect();
//                    CountriesLinkedList * countries;
//                        countries = getCountriesAgg();
//                        CountriesNode* currentC;
//                        currentC = countries->head;
//                        int * fds = findPipeForCountry(currentC->rec->name);
//                        char * packet = new char[bufferSize];
//                        string tempS;
//                        bool notFound=true;
//                        while (currentC != NULL && notFound==true) {
//                         //   cout << "Aggregator, com will use: " << fds[0] << " and " << fds[1] << endl;
//
//
//                            fds = findPipeForCountry(currentC->rec->name);
//                            tempS = line + " " + currentC->rec->name;
//                            strcpy(packet, tempS.c_str());
//
//                            write(fds[0], packet, bufferSize);
//                            
//                            while (1) {
//                                if (read(fds[1], packet, bufferSize) <= 0) { //pairnei tin apantisi
//                                    aggrErrors();
//                                    aggrTotal();
//                                    break;
//                                }
//                                if (strcmp(packet, "###") == 0) { //vrethike to id ara na stamataei
//                                    notFound=false;
//                                    break;}
//                                else if (strcmp(packet, "-") == 0) {
//                                    break;
//                                } else {
//                                    cout << packet << endl;
//                                }
//
//                            }
//
//                            currentC = currentC->next;
//                        }
//                        if(notFound){
//                            aggrErrors();
//                            aggrTotal();
//                            cout<<"Record id not found"<<endl;
//                        }
//                        delete [] packet;
//                
//                }
                }}
                    else if (data[0] == "/numPatientDischarges" || data[0] == "numPatientDischarges") {
                if (data[1] == "" || data[2] == "" || data[3] == "") {
                    cout << "error: you have to enter one virus name,(maybe a country-optional) and two dates" << endl;
                    aggrErrors();
                    aggrTotal();
                } else {
                    aggrTotal();
                    if (data[4] == "") {
                        cout << "Broadcasting to all: " << line << endl;
                        broadcast(line);
                        
                        collectNumPatientWithSelect();
//                        CountriesLinkedList * countries;
//                        countries = getCountriesAgg();
//                        CountriesNode* currentC;
//                        currentC = countries->head;
//                        int * fds = findPipeForCountry(currentC->rec->name);
//                        char * packet = new char[bufferSize];
//                        string tempS;
//                        while (currentC != NULL) {
//                            cout << "Aggregator, com will use: " << fds[0] << " and " << fds[1] << endl;
//
//
//                            fds = findPipeForCountry(currentC->rec->name);
//                            tempS = line + " " + currentC->rec->name;
//                            strcpy(packet, tempS.c_str());
//
//                            write(fds[0], packet, bufferSize);
//
//                            while (1) {
//                                if (read(fds[1], packet, bufferSize) <= 0) { //pairnei tin apantisi
//                                   aggrErrors();
//                                    aggrTotal();
//                                    break;
//                                }
//                                if (strcmp(packet, "-") == 0) {
//                                    break;
//                                } else {
//                                    cout << packet << endl;
//                                }
//
//                            }
//
//                            currentC = currentC->next;
//                        }
//                        delete [] packet;
                    } else {
                        int * fds = findPipeForCountry(data[4]);

                        cout << "Aggregator, com will use: " << fds[0] << " and " << fds[1] << endl;

                        char * packet = new char[bufferSize];

                        strcpy(packet, line.c_str());

                        write(fds[0], packet, bufferSize);

                        while (1) {
                            if (read(fds[1], packet, bufferSize) <= 0) { //pairnei tin apantisi
                                aggrErrors();
                                aggrTotal();
                                break;
                            }
                            if (strcmp(packet, "-") == 0) {
                                break;
                            } else {
                                cout << packet << endl;
                            }

                        }

                        delete [] packet;
                    }
                }}
                else {
                aggrErrors();
                aggrTotal();
                cout << "Invalid Command" << endl;
                cout << "You can use one of the following:" << endl;
                cout << "exit" << endl;
                cout << "listCountries" << endl;
                cout << "searchPatientRecord" << endl;
                cout << "numPatientDischarges" << endl;
                cout << "numPatientAdmissions" << endl;
                cout << "diseaseFrequency" << endl;
                cout << "topk-AgeRanges" << endl;
          //      cout << "topk_Countries" << endl;

            }
        } else { //an vrei eof
            cleanupWorkers();
            cleanupPipes();
            writeLog();
            break;
        }

    }

    return 0;
}
