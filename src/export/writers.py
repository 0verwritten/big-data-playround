import os
from ..base import BaseClass
import json

class Export(BaseClass):
    def write_results_to_csv(self, results: dict, output_dir: str) -> None:
        self.logger.info(f"Початок запису результатів у директорію {output_dir}")

        try:
            os.makedirs(output_dir, exist_ok=True)
            self.logger.info(f"Директорія {output_dir} створена або вже існує")
        except Exception as e:
            self.logger.error(f"Помилка створення директорії {output_dir}: {str(e)}")
            raise

        for query_name, df in results.items():
            try:
                result_path = os.path.join(output_dir, query_name)

                if hasattr(df, 'coalesce') and callable(getattr(df, 'coalesce', None)):
                    df.coalesce(1) \
                        .write \
                        .mode("overwrite") \
                        .option("header", "true") \
                        .option("delimiter", ",") \
                        .csv(result_path)
                else:
                    try:
                        json_result = json.dumps(df, ensure_ascii=False, indent=4)
                        with open(f"{result_path}.json", "w", encoding="utf-8") as json_file:
                            json_file.write(json_result)
                    except Exception as json_error:
                        self.logger.error(f"Помилка перетворення об'єкта для '{query_name}' у JSON: {str(json_error)}")
                        continue

                self.logger.info(f"Результати для '{query_name}' записані у {result_path}")

            except Exception as e:
                self.logger.error(f"Помилка запису результатів для '{query_name}': {str(e)}")

        self.logger.info(f"Запис результатів завершено. Всього записано {len(results)} результатів")