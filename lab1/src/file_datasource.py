from csv import reader
from typing import List
from datetime import datetime
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData
from random import randint


class FileDatasource:
    def __init__(self, accelerometer_filename: str, gps_filename: str, parking_filename: str, rows_to_return: int = 7) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.rows_to_return = rows_to_return

    def file_data_reader(self, path: str):
        while True:
            file = open(path)
            data_reader = reader(file)
            header = next(data_reader)

            for row in data_reader:
                yield row

            file.close()

    def read(self) -> List[AggregatedData]:
        """Метод повертає дані отримані з датчиків"""
        dataList = []
        for i in range(self.rows_to_return):
            parking_data = next(self.parking_data_reader)
            dataList.append(
                AggregatedData(
                    Accelerometer(*next(self.accelerometer_data_reader)),
                    Gps(*next(self.gps_data_reader)),
                    Parking(parking_data[0], Gps(*parking_data[1:])),
                    datetime.now(),
                    randint(1, 100)
                )
            )

        return dataList

    def startReading(self, *args, **kwargs):
        """Метод повинен викликатись перед початком читання даних"""
        self.accelerometer_data_reader = self.file_data_reader(self.accelerometer_filename)
        self.gps_data_reader = self.file_data_reader(self.gps_filename)
        self.parking_data_reader = self.file_data_reader(self.parking_filename)

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        # Не використовується, оскільки читання безкінечне
        pass