#include "query_funcs.h"
#include <fstream>
#include <iomanip>

void execute_query(std::string s, connection *C){
  // Create a transactional object. 
  work W(*C);

  // Execute SQL query
  W.exec(s);
  W.commit();
}

void create_tables(connection *C){
  std::string sql_player = "CREATE TABLE PLAYER (PLAYER_ID SERIAL PRIMARY KEY, TEAM_ID INT NOT NULL, UNIFORM_NUM INT NOT NULL, FIRST_NAME VARCHAR(200) NOT NULL, LAST_NAME VARCHAR(100) NOT NULL, MPG INT DEFAULT 0, PPG INT DEFAULT 0, RPG INT DEFAULT 0, APG INT DEFAULT 0, SPG DECIMAL(10, 1) DEFAULT 0, BPG DECIMAL(10, 1) DEFAULT 0, FOREIGN KEY (TEAM_ID) REFERENCES TEAM(TEAM_ID));";

  std::string sql_team = "CREATE TABLE TEAM(TEAM_ID SERIAL PRIMARY KEY, NAME VARCHAR(100) NOT NULL, STATE_ID INT NOT NULL, COLOR_ID INT NOT NULL, WINS INT DEFAULT 0 NOT NULL, LOSSES INT DEFAULT 0 NOT NULL, FOREIGN KEY (STATE_ID) REFERENCES STATE(STATE_ID), FOREIGN KEY (COLOR_ID) REFERENCES COLOR(COLOR_ID));";

  std::string sql_state = "CREATE TABLE STATE(STATE_ID SERIAL PRIMARY KEY, NAME VARCHAR(100) NOT NULL);";

  std::string sql_color = "CREATE TABLE COLOR(COLOR_ID SERIAL PRIMARY KEY, NAME VARCHAR(100) NOT NULL);";

  execute_query(sql_state, C);
  execute_query(sql_color, C);
  execute_query(sql_team, C);
  execute_query(sql_player, C); 
}

void drop_tables(connection * C){
  std::string drop = "DROP TABLE IF EXISTS PLAYER; DROP TABLE IF EXISTS TEAM; DROP TABLE IF EXISTS STATE; DROP TABLE IF EXISTS COLOR;";
  execute_query(drop, C);
}

std::vector<std::string> split_words(std::string line){
  std::stringstream stream(line);
  std::string word;
  std::vector<std::string> out;

  while(getline(stream, word, ' ')){
    out.push_back(word);
  }

  return out;
}

void parse_player(connection *C, std::string line){
  std::vector<std::string> words = split_words(line);
  add_player(C, std::stoi(words[1]), std::stoi(words[2]), words[3], words[4], std::stoi(words[5]), std::stoi(words[6]), std::stoi(words[7]), std::stoi(words[8]), std::stod(words[9]), std::stod(words[10]));
}

void parse_team(connection *C, std::string line){
  std::vector<std::string> words = split_words(line);
  add_team(C, words[1], std::stoi(words[2]), std::stoi(words[3]), std::stoi(words[4]), std::stoi(words[5]));
}

void parse_state(connection *C, std::string line){
  std::vector<std::string> words = split_words(line);
  add_state(C, words[1]);
}

void parse_color(connection *C, std::string line){
  std::vector<std::string> words = split_words(line);
  add_color(C, words[1]);
}

void parse_rows(connection *C,const char * fname){
  fstream read_file;
  //open file
  read_file.open(fname, ios::in);
  std::string name(fname);
  
  std::string line;
  while(getline(read_file, line)){
    if(name == "player.txt"){
      parse_player(C, line);
    }
    if(name == "team.txt"){
      parse_team(C, line);
    }
    if(name == "state.txt"){
      parse_state(C, line);
    }
    if(name == "color.txt"){
      parse_color(C, line);
    }
  
  }
  
  //close file
  read_file.close();
}

void add_rows(connection * C){ 
  //parse color
  parse_rows(C, "color.txt");
  //parse state
  parse_rows(C, "state.txt");
  //parse team
  parse_rows(C, "team.txt");
  //parse player
  parse_rows(C, "player.txt");
 
}

void add_player(connection *C, int team_id, int jersey_num, string first_name, string last_name, int mpg, int ppg, int rpg, int apg, double spg, double bpg){
  work W(*C); 
  
  std::string query = "INSERT INTO PLAYER(TEAM_ID, UNIFORM_NUM, FIRST_NAME, LAST_NAME, MPG, PPG, RPG, APG, SPG, BPG) VALUES(" + std::to_string(team_id) + ", " + std::to_string(jersey_num) + ", '" + W.esc(first_name) + "', '" + W.esc(last_name) + "', " + std::to_string(mpg) + ", " + std::to_string(ppg) + ", " + std::to_string(rpg) + ", " + std::to_string(apg) + ", " + std::to_string(spg) + ", " + std::to_string(bpg) + ");";
  
  W.exec(query);
  W.commit();
}


void add_team(connection *C, string name, int state_id, int color_id, int wins, int losses){
  work W(*C);
  
  std::string query = "INSERT INTO TEAM (NAME, STATE_ID, COLOR_ID, WINS, LOSSES) VALUES('" + W.esc(name) + "', " + std::to_string(state_id) + ", " + std::to_string(color_id) + ", " + std::to_string(wins) + ", " + std::to_string(losses) + ");";

  W.exec(query);
  W.commit();
}


void add_state(connection *C, string name){
  work W(*C);
  
  std::string query = "INSERT INTO STATE(NAME) VALUES('" + W.esc(name) + "');";
  W.exec(query);
  W.commit();
}


void add_color(connection *C, string name){
  work W(*C);
  
  std::string query = "INSERT INTO COLOR(NAME) VALUES('" + W.esc(name) + "');";
  W.exec(query);
  W.commit();
}


void add_query(int check, int min, int max, std::string & query, std::string row, int & begin){
  if(check == 1){

    if(begin == 0){
      query += " WHERE ";
      begin = 1;
    }  
    else{
      query += " AND ";
    }  
 
    query += row + " BETWEEN " + std::to_string(min) + " AND " + std::to_string(max);
  }

}
void add_query(int check, double min, double max, std::string & query, std::string row, int & begin){
  if(check == 1){

    if(begin == 0){
      query += " WHERE ";
      begin = 1;
    }  
    else{
      query += " AND ";
    }  
 
    query += row + " BETWEEN " + std::to_string(min) + " AND " + std::to_string(max);
  }

}

void query1(connection *C,
	    int use_mpg, int min_mpg, int max_mpg,
            int use_ppg, int min_ppg, int max_ppg,
            int use_rpg, int min_rpg, int max_rpg,
            int use_apg, int min_apg, int max_apg,
            int use_spg, double min_spg, double max_spg,
            int use_bpg, double min_bpg, double max_bpg
            ){
  std::string query = "SELECT * from PLAYER";
  //keep track of query
  int begin = 0;
  
  add_query(use_mpg, min_mpg, max_mpg, query, "MPG", begin);
  add_query(use_ppg, min_ppg, max_ppg, query, "PPG", begin);
  add_query(use_rpg, min_rpg, max_rpg, query, "RPG", begin);
  add_query(use_apg, min_apg, max_apg, query, "APG", begin);
  add_query(use_spg, min_spg, max_spg, query, "SPG", begin);
  add_query(use_bpg, min_bpg, max_bpg, query, "BPG", begin);
   
  query += ";";
  
  //Create a non-transactional object.
  nontransaction N(*C);

  // Execute SQL query
  result R( N.exec( query ));

  //print output
  std::cout << "PLAYER_ID TEAM_ID UNIFORM_NUM FIRST_NAME LAST_NAME MPG PPG RPG APG SPG BPG\n";
  for (result::const_iterator c = R.begin(); c != R.end(); ++c) {
    std::cout << c[0].as<int>() << " " << c[1].as<int>() << " " << c[2].as<int>() << " ";
    std::cout << c[3].as<string>() << " " << c[4].as<string>() << " ";
    std::cout << c[5].as<int>() << " " << c[6].as<int>() << " ";
    std::cout << c[7].as<int>() << " " << c[8].as<int>() << " ";
    std::cout << c[9].as<string>() << " " << c[10].as<string>() << std::endl;
  }
}


void query2(connection *C, string team_color){
  std::string query = "SELECT T.NAME from TEAM AS T, COLOR AS C WHERE C.COLOR_ID = T.COLOR_ID AND C.NAME = '" + team_color + "';";

  //Create a non-transactional object.
  nontransaction N(*C);

  // Execute SQL query
  result R( N.exec( query ));

  //print output
  std::cout << "NAME\n";
  for (result::const_iterator c = R.begin(); c != R.end(); ++c){
    std::cout << c[0].as<string>() << std::endl;
  }
}


void query3(connection *C, string team_name){
    std::string query = "SELECT P.FIRST_NAME, P.LAST_NAME from TEAM AS T, PLAYER AS P WHERE P.TEAM_ID = T.TEAM_ID AND T.NAME = '" + team_name + "' ORDER BY PPG DESC;";

  //Create a non-transactional object.
  nontransaction N(*C);

  // Execute SQL query
  result R( N.exec( query ));

  //print output
  std::cout << "FIRST_NAME LAST_NAME\n";
  for (result::const_iterator c = R.begin(); c != R.end(); ++c){
    std::cout << c[0].as<string>() << " " << c[1].as<string>() << std::endl;
  }
  
}


void query4(connection *C, string team_state, string team_color){
  std::string query = "SELECT DISTINCT P.FIRST_NAME, P.LAST_NAME, P.UNIFORM_NUM from PLAYER AS P, STATE AS S, COLOR AS C, TEAM AS T WHERE P.TEAM_ID = (SELECT T.TEAM_ID FROM TEAM AS T WHERE T.STATE_ID = (SELECT S.STATE_ID FROM STATE AS S WHERE S.NAME = '" + team_state + "') AND T.COLOR_ID = (SELECT C.COLOR_ID FROM COLOR AS C WHERE C.NAME = '" + team_color + "'));";

  //Create a non-transactional object.
  nontransaction N(*C);

  // Execute SQL query
  result R( N.exec( query ));

  //print output
  std::cout << "FIRST_NAME LAST_NAME UNIFORM_NUM\n";
  for (result::const_iterator c = R.begin(); c != R.end(); ++c){
    std::cout << c[0].as<string>() << " " << c[1].as<string>() << " " << c[2].as<int>() << std::endl;
  }
  
}


void query5(connection *C, int num_wins){
  std::string query = "SELECT P.FIRST_NAME, P.LAST_NAME, T.NAME, T.WINS from TEAM AS T, PLAYER AS P WHERE P.TEAM_ID = T.TEAM_ID AND T.WINS > " + std::to_string(num_wins) + ";";

  //Create a non-transactional object.
  nontransaction N(*C);

  // Execute SQL query
  result R( N.exec( query ));

  //print output
  std::cout << "FIRST_NAME LAST_NAME NAME WINS\n";
  for (result::const_iterator c = R.begin(); c != R.end(); ++c){
    std::cout << c[0].as<string>() << " " << c[1].as<string>() << " ";
    std::cout << c[2].as<string>() << " " << c[3].as<int>() << std::endl;
  }
}
