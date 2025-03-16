import configparser
import json
import requests

import database.database_handler


class TableTennisScraper:
    @staticmethod
    def get_text_data(url_template: str, *args, **kwargs) -> str:
        res = requests.get(url_template.format(*args, **kwargs))
        return res.text

    @staticmethod
    def get_json_data(url_template: str, *args, **kwargs) -> json:
        res = requests.get(url_template.format(*args, **kwargs))
        return json.loads(res.text)

    @staticmethod
    def get_customized_data(data: json) -> list:
        """depends on source and table format"""
        res = []
        for arena in range(len(data)):
            for match in data[arena]["matches"]:
                player_one = [match["id"],
                              match["tournamentName"],
                              match["player1"]["firstName"] + " " + match["player1"]["lastName"],
                              match["player1Score"],
                              match["startDate"][: 10],
                              match["startDate"][12: 19]]
                for set_score in match["setScores"]:
                    player_one.append(set_score["p1Score"])
                for i in range(7 - len(match["setScores"])):
                    player_one.append(None)

                player_two = [match["id"],
                              match["tournamentName"],
                              match["player2"]["firstName"] + " " + match["player2"]["lastName"],
                              match["player2Score"],
                              match["startDate"][: 10],
                              match["startDate"][12: 19]]
                for set_score in match["setScores"]:
                    player_two.append(set_score["p2Score"])
                for i in range(7-len(match["setScores"])):
                    player_two.append(None)

                res.append(player_one)
                res.append(player_two)

        return res

    def run(self) -> None:
        config = configparser.ConfigParser()
        config.read("table_tennis_scraper.config")
        host = config["config"]["host"]
        user = config["config"]["user"]
        pw = config["config"]["pw"]
        db = config["config"]["db"]
        table = config["config"]["table"]
        columns = config["config"]["columns"]
        insert_statement_template = config["config"]["insert_statement_template"]
        url_template = config["config"]["url_template"]

        dh = database.database_handler.DatabaseHandler()
        dh.check_and_create_database(db, host, user, pw)
        conn = dh.get_connection(host, user, pw, db)
        dh.check_and_create_table(table, columns, conn=conn, disconnect=False)

        # TODO: add for loops for date and period
        data = self.get_customized_data(self.get_json_data(url_template, date="2025-02-05", period="2"))
        dh.insert(insert_statement_template, data, conn=conn, disconnect=False)

        conn.close()


if __name__ == "__main__":
    table_tennis_scraper = TableTennisScraper()
    table_tennis_scraper.run()
