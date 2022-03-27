#include <iostream>
#include "mpi.h"
#include <Windows.h>
#include <fstream>
#include <cstdlib>
#include <time.h>
#include <fstream>

//mpiexec -n 8 "B:\3.1 Parralel\MatrixMultiplay\MatrixMultiplayPath\x64\Debug\MPIMatrixMultiply.exe"

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
#define typeQueryFinalWork 0

//Типы ответов
#define work 5
#define miss 10
#define finalize -10

using namespace std;


int randomRange(int max, int min = 1) {
	return min + rand() % (max - min);
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
	sprintf_s(filenameDataBase, "B:\\3.1 Parralel\\MatrixMultiplay\\MatrixMultiplayPath\\Randevu\\logDatabase.txt");

	fstream dataBase(filenameDataBase, ios::binary | ios::app);

	//Ошибка открытия файлов
	if (!dataBase.is_open()) {
		cout << "Database don't avaliable!" << endl;
		MPI_Finalize();
		return 1;
	}

	//Сервер
	if (rank == 0) {
		int	nowWrite = -1;
		int counter = 0;
		
		while (true) {
			int buf;
			//Конец по числу запросов
			if (counter >= maxQueries)break;
			//Проверяем наличие запроса
			int flag = false;
			MPI_Iprobe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &flag, &status);
			if (flag) {
				counter++;
				MPI_Recv(&buf, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD, &status);
				//Проверка о завершении записи
				if (nowWrite == status.MPI_SOURCE) nowWrite = -1;
				//Обработка запроса на запись
				else if (buf == typeQueryWrite) {
					//Можно писать
					if (nowWrite < 0) {
						buf = work;
						cout << "User " << status.MPI_SOURCE << " start WRITE to file!" << endl;
						nowWrite = status.MPI_SOURCE;
						MPI_Send(&buf, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
					}
					//Запись занята
					else {
						buf = miss;
						cout << "User try " << status.MPI_SOURCE << " start WRITE to file, but file occupied!" << endl;
						MPI_Send(&buf, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
					}
				}
				//Обработка запроса на чтение
				else {
					buf = work;
					cout << "User " << status.MPI_SOURCE << " start READ to file!" << endl;
					MPI_Send(&buf, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
				}
			}
		}
		//Завершаем работу всех потоков
		int buf = finalize;
		cout << "Start send finalize!" << endl;
		for (int i = 1; i < size;i++) {
			MPI_Recv(&nowWrite, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
			if(nowWrite == status.MPI_SOURCE)MPI_Recv(&nowWrite, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
			cout << "Send finalize to " << status.MPI_SOURCE << endl;
			buf = finalize;
			MPI_Send(&buf, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
		}
	}
	//Пользователи
	else {
		int type = typeReader;
		streampos begin = rank;
		if (rank < countWriters + 1)type = typeWriter;
		while (true) {
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
			else if (answer == work) {
				//Выполняем работу с базой данных
				if (type == typeWriter) {
					//Генерируем число и записываем в файд
					typeQuery = randomRange(2000, 1500);
					dataBase << "Log | User " << rank << ": I generate " << typeQuery << " data blocks" << endl;
					//Отправляем ответ о завершённой работе с базой данных					
					MPI_Send(&typeQuery, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
				}
				else {
					//Находим объём файла от текущей позиции
					begin = dataBase.tellg();
					dataBase.seekg(0, ios::end);
					typeQuery = dataBase.tellg() - begin;
				}
			}
		}
	}
	dataBase.close();
	MPI_Finalize();
}