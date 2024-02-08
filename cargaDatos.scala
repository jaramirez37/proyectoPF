import cats.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import cats.implicits.*
import com.github.tototoshi.csv.*
import doobie.*
import doobie.implicits.*
import java.io.File

implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object cargaDatos {

  @main
  def exportarDatos2BD(): Unit =
    val path2DataFile1 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dsPartidosYGoles.csv"
    val reader1 = CSVReader.open(new File(path2DataFile1))
    val contentFilePyG: List[Map[String, String]] =
      reader1.allWithHeaders()
    reader1.close()

    val path2DataFile2 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dsAlineacionesXTorneo.csv"
    val reader2 = CSVReader.open(new File(path2DataFile2))
    val contentFileAxT: List[Map[String, String]] =
      reader2.allWithHeaders()
    reader2.close()

    val path2DataFile3 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dataset\\Away_Teams.csv"
    val reader3 = CSVReader.open(new File(path2DataFile3))
    val contentFile3: List[Map[String, String]] =
      reader3.allWithHeaders()
    reader3.close()

    val path2DataFile4 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dataset\\Goals.csv"
    val reader4 = CSVReader.open(new File(path2DataFile4))
    val contentFile4: List[Map[String, String]] =
      reader4.allWithHeaders()
    reader4.close()

    val path2DataFile5 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dataset\\Home_Teams.csv"
    val reader5 = CSVReader.open(new File(path2DataFile5))
    val contentFile5: List[Map[String, String]] =
      reader5.allWithHeaders()
    reader5.close()

    val path2DataFile6 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dataset\\Matches.csv"
    val reader6 = CSVReader.open(new File(path2DataFile6))
    val contentFile6: List[Map[String, String]] =
      reader6.allWithHeaders()
    reader6.close()

    val path2DataFile7 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dataset\\Players.csv"
    val reader7 = CSVReader.open(new File(path2DataFile7))
    val contentFile7: List[Map[String, String]] =
      reader7.allWithHeaders()
    reader7.close()

    val path2DataFile8 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dataset\\Squads.csv"
    val reader8 = CSVReader.open(new File(path2DataFile8))
    val contentFile8: List[Map[String, String]] =
      reader8.allWithHeaders()
    reader8.close()

    val path2DataFile9 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dataset\\Stadiums.csv"
    val reader9 = CSVReader.open(new File(path2DataFile9))
    val contentFile9: List[Map[String, String]] =
      reader9.allWithHeaders()
    reader9.close()

    val path2DataFile10 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dataset\\Tournaments.csv"
    val reader10 = CSVReader.open(new File(path2DataFile10))
    val contentFile10: List[Map[String, String]] =
      reader10.allWithHeaders()
    reader10.close()

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/proyectopf",
      user = "root",
      password = "Joxexph.1203",
      logHandler = None
    )
    generateDataPlayers(contentFile7) // Genera y procesa datos de jugadores del contenido del archivo `contentFileAxT`
      .foreach(insert => insert.run.transact(xa).unsafeRunSync()) // Para cada inserción generada, ejecuta la transacción y espera la finalización
    generateDataTeams(contentFilePyG)
      .foreach(insert => insert.run.transact(xa).unsafeRunSync())
    generateDataTournaments(contentFile10)
      .foreach(insert => insert.run.transact(xa).unsafeRunSync())
    generateDataStadiums(contentFile9)
      .foreach(insert => insert.run.transact(xa).unsafeRunSync())
    generateDataAlignments(contentFile8)
      .foreach(insert => insert.run.transact(xa).unsafeRunSync())
    generateDataMatches(contentFile6)
      .foreach(insert => insert.run.transact(xa).unsafeRunSync())
    generateDataGoals(contentFile4)
      .foreach(insert => insert.run.transact(xa).unsafeRunSync())

  // Define la función `generateDataPlayers` que toma una lista de mapas
  def generateDataPlayers(data: List[Map[String, String]]): List[doobie.Update0] =
    val playerTuple = data
      .map(                           // Mapea los datos de entrada a una lista de tuplas
        row => (
          row("players_birth_date"),
          row("players_defender"),
          row("players_family_name"),
          row("players_female"),
          row("players_forward"),
          row("players_given_name"),
          row("players_goal_keeper"),
          row("players_midfielder")
        )
      )
      .distinct //Eliminar Duplicados
      .map(t7 => // Mapea cada tupla a una sentencia SQL de inserción y crea una lista de Update0
        sql""" INSERT INTO players(players_birth_date, players_defender, players_family_name, players_female,
               players_forward, players_given_name, players_goal_keeper, players_midfielder)
               VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5}, ${t7._6} ,${t7._7}, ${t7._8})
           """.update)
    playerTuple // Devuelve la lista de sentencias SQL de inserción

  def generateDataMatches(data: List[Map[String, String]]): List[doobie.Update0] =
    val matchTuple = data
      .map(
        row => (row("matches_match_id"),
          row("matches_tournament_id"),
          row("matches_away_team_id"),
          row("matches_home_team_id"),
          row("matches_stadium_id"),
          row("matches_match_date"),
          row("matches_match_time"),
          row("matches_stage_name"),
          row("matches_home_team_score"),
          row("matches_away_team_score"),
          row("matches_extra_time"),
          row("matches_penalty_shootout"),
          row("matches_home_team_score_penalties"),
          row("matches_away_team_score_penalties"),
          row("matches_result"))
      )
      .distinct
      .map(t7 =>
        sql""" INSERT INTO matches(matches_match_id, matches_tournament_id, matches_away_team_id, matches_home_team_id, matches_stadium_id,
              matches_match_date, matches_match_time, matches_stage_name, matches_home_team_score, matches_away_team_score,
              matches_extra_time, matches_penalty_shootout, matches_home_team_score_penalties, matches_away_team_score_penalties,
              matches_result)
               VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5}, ${t7._6} ,${t7._7}, ${t7._8},${t7._9},${t7._10},
               ${t7._11},${t7._12},${t7._13},${t7._14},${t7._15})
           """.update)
    matchTuple

  def generateDataGoals(data: List[Map[String, String]]): List[doobie.Update0] =
    val goalTuple = data
      .map(
        row => (row("goals_goal_id").trim,
          row("matches_match_id"),
          row("goals_team_id"),
          row("goals_player_id"),
          row("matches_tournament_id"),
          row("goals_minute_label"),
          row("goals_minute_regulation"),
          row("goals_minute_stoppage"),
          row("goals_match_period"),
          row("goals_own_goal"),
          row("goals_penalty"))
      )
      .filterNot(_._7 == "NA")
      .distinct
      .map(t7 =>
        sql""" INSERT INTO goals(goals_goal_id, matches_match_id, goals_team_id, goals_player_id, matches_tournament_id,
               goals_minute_label, goals_minute_regulation, goals_minute_stoppage, goals_match_period, goals_own_goal,
               goals_penalty)
               VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5}, ${t7._6} ,${t7._7}, ${t7._8},${t7._9},${t7._10},
               ${t7._11})
           """.update)
    goalTuple

  def generateDataAlignments(data: List[Map[String, String]]): List[doobie.Update0] =
    val alignmentTuple = data
      .map(
        row => (row("squads_player_id").trim,
          row("squads_position_name"),
          row("squads_shirt_number"),
          row("squads_team_id"),
          row("squads_tournament_id"))
      )
      .distinct
      .map(t7 =>
        sql""" INSERT INTO alignments(squads_player_id, squads_position_name, squads_shirt_number, squads_team_id,
               squads_tournament_id)
               VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5})
           """.update)
    alignmentTuple


  def generateDataStadiums(data: List[Map[String, String]]): List[doobie.Update0] =
    val stadiumTuple = data
      .map(
        row => (row("matches_stadium_id").trim,
          row("stadiums_city_name"),
          row("stadiums_country_name"),
          row("stadiums_stadium_capacity"),
          row("stadiums_stadium_name"))
      )
      .distinct
      .map(t7 =>
        sql""" INSERT INTO stadiums(matches_stadium_id, stadiums_city_name, stadiums_country_name,
               stadiums_stadium_capacity, stadiums_stadium_name)
               VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5})
           """.update)
    stadiumTuple

  def generateDataTeams(data: List[Map[String, String]]): List[doobie.Update0] =
    val teamTuple = data
      .map(
        row => (row("matches_away_team_id"),
          row("away_team_name"),
          row("away_region_name"),
          row("away_mens_team"),
          row("away_womens_team"))
      )
      .distinct
      .map(t7 =>
        sql""" INSERT INTO teams(team_id, team_name, team_region_name, men_team, women_team)
               VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5})
           """.update)
    teamTuple

  def generateDataTournaments(data: List[Map[String, String]]) =
    val tournamentsTuple = data
      .map(
        row => (row("matches_tournament_id"),
          row("tournaments_tournament_name"),
          row("tournaments_year"),
          row("tournaments_host_country"),
          row("tournaments_winner"),
          row("tournaments_count_teams"))
      )
      .distinct
      .map(t7 =>
        sql""" INSERT INTO tournaments(matches_tournament_id, tournaments_tournament_name, tournaments_year,
               tournaments_host_country, tournaments_winner, tournaments_count_teams)
               VALUES(${t7._1}, ${t7._2}, ${t7._3}, ${t7._4}, ${t7._5},${t7._6})
           """.update)
    tournamentsTuple
}