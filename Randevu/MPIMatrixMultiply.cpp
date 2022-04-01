#include <iostream>
#include "mpi.h"
#include <Windows.h>
#include <fstream>
#include <cstdlib>
#include <time.h>
#include <fstream>
#include <string>

//mpiexec -n 8 "C:\Parallel 3.2\ReadersWriters\x64\Debug\MPIMatrixMultiply.exe"

//Настройки
#define countWriters 2
#define maxQueries 32

//Типы пользователей
#define typeWriter 1000
#define typeReader 2000

//Типы запросов
#define typeQueryWrite 10
#define typeQueryRead 20
#define typeQueryComplete 25


//Типы ответов
#define work 5
#define miss 10
#define wait 15
#define finalize -10

using namespace std;


int randomRange(int max, int min = 1) {
	return min + rand() % (max - min);
}

void WriteToDB(fstream *database, int target) {
	//Генерируем число, находим значение функции и передаём для записи
	int point = randomRange(2000, 1500);
	double buf = point * 15 - 4 * sin(point) + log(point);
	*database << "Log | User " << target << ": Function result = " << buf << " in " << point << " point" << endl;
}

int NormalizeQueue(int * queue, int length) {
	int end = 0;
	int now;
	for (int i = 0; i < length;i++) {
		if (queue[i] > 0) {
			now = queue[i];
			queue[i] = -1;
			queue[end] = now;
			end++;
		}
	}
	return end;
}

bool RemoveFromQueue(int* queue, int length, int target) {
	bool result = false;
	for (int i = 0; i < length; i++) {
		if (queue[i] == target) {
			queue[i] = -1;
			result = true;
			break;
		}
	}
	return result;
}

void AnswerWriter(int target, int* listReaders, int countReaders) {
	int answer = wait;
	//Закрыть доступ всем, сейчас активным читателям
	for (int i = 0; i < countReaders; i++)
		if (listReaders[i] > 0) {
			cout << "Pause read " << listReaders[i] << endl;
			MPI_Send(&answer, 1, MPI_INT, listReaders[i], 0, MPI_COMM_WORLD);
		}
	answer = work;
	MPI_Send(&answer, 1, MPI_INT, target, 0, MPI_COMM_WORLD);
}

void CommandToReaders(int command, int* listReaders, int countReaders) {
	cout << "0 | All readers command: " << command << endl;
	for (int i = 0; i < countReaders; i++)
		if (listReaders[i] > 0) {
			MPI_Send(&command, 1, MPI_INT, listReaders[i], 0, MPI_COMM_WORLD);
		}
}

void ShowQueue(int* list, int count) {
	for (int i = 0; i < count; i++)
		cout << " " << list[i];
	cout << endl;
}


int main(int argv, char** argc)
{
	int size, rank;
	if (MPI_Init(&argv, &argc) != MPI_SUCCESS)//Проверка на инициализацию
		return 1;
	if (MPI_Comm_size(MPI_COMM_WORLD, &size) != MPI_SUCCESS)//Получение размера коммуникатора
		return 2;
	if (MPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS)//Получение текущего ранга 
		return 3;
	if (size < 2)return 4;
	MPI_Status status;


	srand(rank);

	char filenameDataBase[256];
	sprintf_s(filenameDataBase, "C:\\Parallel 3.2\\ReadersWriters\\Randevu\\logDatabase.txt");

	fstream dataBase(filenameDataBase, ios::binary | ios::app);

	//Ошибка открытия файлов
	if (!dataBase.is_open()) {
		cout << "Database don't avaliable!" << endl;
		MPI_Finalize();
		return 1;
	}

	//Сервер
	if (rank == 0) {
		int counter = 0;
		int countReaders = size - countWriters - 1;
		bool readersOnPause = false;
		bool nowWrite = false;
		//Активные читатели и очередь писателей
		int* activeReaders = new int[countReaders];
		int* queueWriters = new int[countWriters];
		for (int i = 0; i < countWriters; i++) queueWriters[i] = -1;
		for (int i = 0; i < countReaders; i++) activeReaders[i] = -1;
		int currentReaders, currentWriters;

		while (true) {
			if (counter > maxQueries)break;
			int query, answer;

			currentReaders = NormalizeQueue(activeReaders, countReaders);
			currentWriters = NormalizeQueue(queueWriters, countWriters);
			
			if (currentWriters < 1) {
				//Снимаем с паузы
				if (readersOnPause) {
					CommandToReaders(work, activeReaders, countReaders);
					readersOnPause = false;
				}
			}
			else {
				if (!nowWrite) {
					answer = work;
					if (!readersOnPause) {
						CommandToReaders(wait, activeReaders, countReaders);
						readersOnPause = true;
					}
					cout << queueWriters[0] << " | answer by queue: " << answer << endl;
					MPI_Send(&answer, 1, MPI_INT, queueWriters[0], 0, MPI_COMM_WORLD);
					nowWrite = true;
				}
			}

			int flag = false;
			MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &flag, &status);
			if (flag) {
				cout << "=======" << endl;
				cout << "Readers: ";
				ShowQueue(activeReaders, countReaders);
				cout << "Writers: ";
				ShowQueue(queueWriters, countWriters);
				counter++;
				MPI_Recv(&query, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
				cout << status.MPI_SOURCE << " | query: " << query << endl;
				switch (query)
				{
				case typeQueryRead:
					activeReaders[currentReaders] = status.MPI_SOURCE;
					answer = wait;
					if (currentWriters < 1)answer = work;
					MPI_Send(&answer, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
					break;
				case typeQueryWrite:
					queueWriters[currentWriters] = status.MPI_SOURCE;
					answer = wait;
					MPI_Send(&answer, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
					break;
				case typeQueryComplete:
					answer = -1;
					if (RemoveFromQueue(queueWriters, countWriters, status.MPI_SOURCE))nowWrite = false;
					else RemoveFromQueue(activeReaders, countReaders, status.MPI_SOURCE);
					break;
				default:
					break;
				}
				cout << status.MPI_SOURCE << " | answer: " << answer << endl;
				cout << "Readers: ";
				ShowQueue(activeReaders, countReaders);
				cout << "Writers: ";
				ShowQueue(queueWriters, countWriters);
			}
		}
		//Завершаем работу всех потоков
		cout << "Start send finalize!" << endl;
		cout << "Readers: ";
		ShowQueue(activeReaders, countReaders);
		cout << "Writers: ";
		ShowQueue(queueWriters, countWriters);

		int buf = finalize;
		int query;
		
		int i = 1;
		for (int j = 0; j < countReaders; j++)
			if (activeReaders[j] > 0) {
				if(!readersOnPause)MPI_Recv(&query, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
			}
		for (int j = 0; j < countWriters; j++)
			if (queueWriters[j] > 0) {
				MPI_Send(&buf, 1, MPI_INT, queueWriters[j], 0, MPI_COMM_WORLD);
				cout << "Send finalize to by queue " << queueWriters[j] << endl;
				i++;
			}

		for (; i < size;i++) {
			buf = finalize;
			MPI_Recv(&query, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
			cout << "Send finalize to " << status.MPI_SOURCE << endl;
			MPI_Send(&buf, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
		}
	}
	//Пользователи
	else {
		//Определение типа процесса
		int type = typeReader;
		int typeQuery, answer;
		if (rank < countWriters + 1)type = typeWriter;
		bool stopper = false;
		char target[256];
		sprintf_s(target, "C:\\Parallel 3.2\\ReadersWriters\\Randevu\\reader_%d.txt", rank);

		ofstream localDB(target, ios::binary | ios::app);

		while (true) {
			if (stopper)break;
			Sleep(randomRange(250, 100));
			//Тип запроса
			if (type == typeWriter) typeQuery = typeQueryWrite;
			else typeQuery = typeQueryRead;
			//Отравляем тип запроса
			MPI_Send(&typeQuery, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
			//Получаем ответ
			MPI_Recv(&answer, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			while(answer == wait)MPI_Recv(&answer, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			if (answer == finalize)break;
			else {
				if (type == typeWriter) {
					WriteToDB(&dataBase, rank);
					//Отправляем ответ с данными и завершённой работе с базой данных
					typeQuery = typeQueryComplete;
					MPI_Send(&typeQuery, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
				}
				else {
					//Находим значение функции
					string str;
					int counter = 0;
					dataBase.close();
					ifstream dataBase(filenameDataBase);
					while (getline(dataBase, str)) {
						int flag = false;
						MPI_Iprobe(0, 0, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
						if (flag) {
							MPI_Recv(&answer, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
							while (answer == wait)MPI_Recv(&answer, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
							if (answer == finalize) {
								localDB << "Finalize!" << endl;
								stopper = true;
								break;
							}
						}
						//Чтение
						counter++;
					}
					localDB << "String final now: " << counter << endl;
					if (stopper)break;
					//Запрос о завершении чтения
					answer = typeQueryComplete;
					MPI_Send(&answer, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
					if(type == typeWriter)Sleep(randomRange(500, 250));
				}
			}
		}
		if (type == typeReader)localDB.close();
	}
	dataBase.close();
	MPI_Finalize();
}

