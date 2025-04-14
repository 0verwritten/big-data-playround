import os
from ..base import BaseClass

class Export(BaseClass):
    def write_results_to_csv(self, results: dict, output_dir: str) -> None:
        self.logger.info(f"Початок запису результатів у директорію {output_dir}")

        # Створення директорії для результатів, якщо вона не існує
        try:
            os.makedirs(output_dir, exist_ok=True)
            self.logger.info(f"Директорія {output_dir} створена або вже існує")
        except Exception as e:
            self.logger.error(f"Помилка створення директорії {output_dir}: {str(e)}")
            raise

        # Запис кожного результату
        for query_name, df in results.items():
            try:
                result_path = os.path.join(output_dir, query_name)

                # Запис у CSV
                df.coalesce(1) \
                    .write \
                    .mode("overwrite") \
                    .option("header", "true") \
                    .option("delimiter", ",") \
                    .csv(result_path)

                self.logger.info(f"Результати для '{query_name}' записані у {result_path}")

            except Exception as e:
                self.logger.error(f"Помилка запису результатів для '{query_name}': {str(e)}")
                # Продовжуємо з наступним результатом

        self.logger.info(f"Запис результатів завершено. Всього записано {len(results)} результатів")