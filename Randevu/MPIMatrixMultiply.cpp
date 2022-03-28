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
#define ndim 2// size a^2
#define countWriters 3
#define maxQueries 32

//Типы пользователей
#define typeWriter 1000
#define typeReader 2000

//Типы запросов
#define typeQueryWrite 10
#define typeQueryRead 20
#define typeQueryFinalRead 25
#define typeQueryFinalWork 0


//Типы ответов
#define work 5
#define miss 10
#define wait 15
#define finalize -10

using namespace std;


int randomRange(int max, int min = 1) {
	return min + rand() % (max - min);
}

void SendDataRange(int start, int end, int type) {
	for (int i = start; i < end; i++) {
		MPI_Send(&type, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
	}
}

int NormalizeQueue(int * queue, int length) {
	int end = 0;
	for (int i = 0; i < length;i++) {
		if (queue[i] > 0) {
			queue[end] = queue[i];
			end++;
		}
	}
	return end;
}

void AnswerWriter(int target, int* listReaders, int countReaders) {
	int answer = wait;
	//Закрыть доступ всем, сейчас активным читателям
	for (int i = 0; i < countReaders; i++)
		if (listReaders[i] > 0)
			MPI_Send(&answer, 1, MPI_INT, listReaders[i], 0, MPI_COMM_WORLD);
	answer = work;
	MPI_Send(&answer, 1, MPI_INT, target, 0, MPI_COMM_WORLD);
}

void AnswerRead() {


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
		//Информация и очередь
		int* listReaders = new int[size-countWriters-1];
		int* queueWriters = new int[countWriters];
		int nowWritting = -1;
		for (int i = 0; i < countWriters; i++) queueWriters[i] = -1;
		for (int i = 0; i < countReaders; i++) listReaders[i] = -1;

		while (true) {
			if (counter > maxQueries)break;
			int query, answer;
			//Проверка запроса
			int flag = false;
			MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &flag, &status);
			if (flag) {
				counter++;
				MPI_Recv(&query, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
				if (nowWritting > 0) {
					//Закончил запись
					if (nowWritting == status.MPI_SOURCE) {
						nowWritting = -1;
						//Работа с очередью
						int writersWaiting = NormalizeQueue(queueWriters, countWriters);
						if (writersWaiting > 0) {
							nowWritting = queueWriters[0];
							AnswerWriter(nowWritting, listReaders, countReaders);
						}
						else {
							int answer = work;
							//Открыть доступ всем, сейчас активным читателям
							for (int i = 0; i < countReaders; i++)
								if (listReaders[i] > 0)
									MPI_Send(&answer, 1, MPI_INT, listReaders[i], 0, MPI_COMM_WORLD);
						}
					}
					//Запись ещё не закончена
					else {
						answer = wait;
						if (query == typeQueryWrite) {
							cout << status.MPI_SOURCE << " add to writers queue" << endl;
							queueWriters[NormalizeQueue(queueWriters, countWriters)] = status.MPI_SOURCE;
							MPI_Send(&answer, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
						}
						else if (query == typeQueryRead) {
							cout << status.MPI_SOURCE << " add to readers queue" << endl;
							listReaders[NormalizeQueue(listReaders, countReaders)] = status.MPI_SOURCE;
							MPI_Send(&answer, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
						}
					}
				}
				else {
					if (query == typeQueryWrite) {
						cout << "Now write: " << status.MPI_SOURCE << endl;
						nowWritting = status.MPI_SOURCE;
						AnswerWriter(nowWritting, listReaders, countReaders);
					}
					else if(query == typeQueryFinalRead) {
						cout << "Stop read: " << status.MPI_SOURCE << endl;
						for (int i = 0; i < NormalizeQueue(listReaders, countReaders); i++)
							if (listReaders[i] == status.MPI_SOURCE) {
								listReaders[i] = -1;
								break;
							}
					}
					else {
						cout << "Start read: " << status.MPI_SOURCE << endl;
						answer = work;
						listReaders[NormalizeQueue(listReaders, countReaders)] = status.MPI_SOURCE;
						MPI_Send(&answer, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
					}
				}
			}
		}
		//Завершаем работу всех потоков
		int buf = finalize;
		int query;
		cout << "Start send finalize!" << endl;
		int i = 1;
		for (int j = 0; j < countReaders; j++)
			if (listReaders[j] > 0) {
				MPI_Send(&buf, 1, MPI_INT, listReaders[j], 0, MPI_COMM_WORLD);
				i++;
			}
		for (int j = 0; j < countWriters; j++)
			if (queueWriters[j] > 0) {
				MPI_Send(&buf, 1, MPI_INT, queueWriters[j], 0, MPI_COMM_WORLD);
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
		int type = typeReader;
		streampos begin = rank;
		bool stopper = false;
		if (rank < countWriters + 1)type = typeWriter;
		while (true) {
			if (stopper)break;
			Sleep(randomRange(3000, 300));
			int typeQuery, answer;
			//Писатель
			if (type == typeWriter) typeQuery = typeQueryWrite;
			//Читатель
			else typeQuery = typeQueryRead;
			//Отравляем тип запроса
			MPI_Send(&typeQuery, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
			//Получаем ответ
			MPI_Recv(&answer, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			if (answer == finalize)break;
			else if (answer == work || answer == wait) {
				if (answer == wait) {
					MPI_Recv(&answer, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					if (answer == finalize)break;
				}
				//Выполняем работу с базой данных
				if (type == typeWriter) {
					//Генерируем число, находим значение функции и передаём для записи
					typeQuery = randomRange(2000, 1500);
					double buf = typeQuery * 15 - 4 * sin(typeQuery) + log(typeQuery);
					dataBase << "Log | User " << rank << ": Function result = " << buf << " in " << typeQuery << " point" << endl;
					//Отправляем ответ с данными и завершённой работе с базой данных
					MPI_Send(&typeQuery, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
				}
				else {
					//Находим значение функции
					MPI_Recv(&answer, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					string str;
					int counter = 0;
					int flag = false;
					while (getline(dataBase, str)) {
						MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &flag, &status);
						if (flag) {
							//Для остановки
							MPI_Recv(&answer, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
							if (answer == finalize) {
								stopper = true;
								break;
							}
							//Для продолжения
							MPI_Recv(&answer, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
						}
						//Чтение
						counter++;
					}
					if (stopper)break;
					//Запрос о завершении чтения
					MPI_Send(&counter, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
				}
			}
		}
	}
	dataBase.close();
	MPI_Finalize();
}