import cats._
import cats.effect._
import cats.implicits._
import cats.effect.unsafe.implicits.global
import com.github.tototoshi.csv._
import doobie._
import doobie.implicits._
import java.io.File
import org.nspl._
import org.nspl.awtrenderer._
import org.nspl.data.HistogramData
import org.nspl.data.HistogramData._
import org.nspl.{data, *}
import org.saddle.{Index, Series, Vec}
import scala.concurrent.ExecutionContext.Implicits.global

implicit object CustomFormat extends DefaultCSVFormat {
  override val delimiter: Char = ';'
}

object proyectoPF {

  @main
  def work(): Unit = {
    val path2DataFile1 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dsPartidosYGoles.csv"
    val reader1 = CSVReader.open(new File(path2DataFile1))// Abre el archivo CSV para lectura.
    val contentFilePyG: List[Map[String, String]] =
      reader1.allWithHeaders() //Lo almacena en contentFilePyG como una lista de mapas.
    reader1.close()
    println("Información del primer archivo PyG:")
    println(s"Filas: ${contentFilePyG.length} y Columnas: ${contentFilePyG(0).keys.size}")

    val path2DataFile2 = "C:\\Users\\josep\\OneDrive\\Escritorio\\dsAlineacionesXTorneo.csv"
    val reader2 = CSVReader.open(new File(path2DataFile2))
    val contentFileAxT: List[Map[String, String]] =
      reader2.allWithHeaders()
    reader2.close()
    println("Información del segundo archivo archivo AxT:")
    println(s"Filas: ${contentFileAxT.length} y Columnas: ${contentFileAxT(0).keys.size}")

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/proyectopracticum",
      user = "root",
      password = "Joxexph.1203",
      logHandler = None
    )
    // Comprobacion de la coneccion a la base de datos
    val program = sql"SELECT 1".query[Int].unique.transact(xa)
    program.attempt.unsafeRunSync() match {
      case Left(error) =>
        println(s"Error al conectar a la base de datos: $error")
      case Right(_) =>
        println("Conexión a la base de datos exitosa")
    }
    //Generacion de Imagen por Archivos
    chartingGxMinute(contentFilePyG) //GxM
    chartBarPlot(contentFilePyG) //GxT
    chartBarQWandM(contentFileAxT) //PxA
    //Generacion de Imagen por Base de datos
    chartBarPlotStadium(xa) //PxE
    chartBarTeamsGenre(xa) //ExMvF
    chartBarWTournaments(xa) //TxA

    //Generar Imagen Goles por minuto
    def chartingGxMinute(data: List[Map[String, String]]): Unit = {
      val data4Chart = data
        .filter(row => row("goals_minute_regulation") != "NA")// Filtra los datos para eliminar las filas donde el valor de "goals_minute_regulation" es "NA".
        .map(row => (row("goals_minute_regulation").toDouble, row("goals_goal_id")))// Mapea los datos para obtener una lista de pares como valores Double.
        .map(x => x._1 -> x._2)
        .groupBy(_._1) //Agrupo datos del primer valor
        .map(x => (x._1.toString, x._2.length.toDouble)) // Mapea los datos para convertir la longitud de cada grupo en Double.
      val indices = Index(data4Chart.map(value => value._1).toArray) // Crea un índice a partir de las claves de los datos.
      val values = Vec(data4Chart.map(value => value._2).toArray) // Crea un vector de valores a partir de las frecuencias de goles por minuto.
      val series = Series(indices, values)
      // Crea un gráfico de barras horizontales utilizando los datos de la serie, con opciones de estilo y etiquetas especificadas.
      val bar1 = saddle.barplotHorizontal(series,
        xLabFontSize = Option(RelFontSize(0.4)),
        color = RedBlue(86, 186))(
        par
          .xlab("Minuto")
          .ylab("freq.")
          .xLabelRotation(-77)
          .xNumTicks(0)
          .main("Goles por Minuto"))
      pngToFile(new File("C:\\Users\\josep\\IdeaProjects\\proyectoPF\\Imagenes\\Archivos\\GxM.png"), bar1.build, 400)
    }

    // Generar Imagen Goles por Torneo Masculino
    def chartBarPlot(data: List[Map[String, String]]): Unit = {
      val data4Chart: List[(String, Double)] = data.filter(_("tournaments_tournament_name").contains("Men")) // Filtra los datos para quedarse solo con los torneos masculinos.
        // Mapea los datos para obtener una lista de tuplas con información relevante de los partidos.
        .map(row => (
          row("tournaments_tournament_name"),
          row("matches_match_id"),
          row("matches_home_team_score"),
          row("matches_away_team_score")
        ))
        .distinct // Elimina duplicados de la lista.
        .map(t4 => (t4._1, t4._3.toInt + t4._4.toInt)) // Mapea los datos para calcular el total de goles por torneo.
        .groupBy(_._1) // Agrupa los datos por el nombre del torneo.
        .map(t2 => (t2._1, t2._2.map(_._2).sum.toDouble)) // Mapea los datos para convertir la suma de goles en Double.
        .toList // Convierte el resultado en una lista.
        .sortBy(_._1) // Ordena la lista por el nombre del torneo.
      val indices = Index(data4Chart.map(value => value._1).toArray)
      val values = Vec(data4Chart.map(value => value._2).toArray)
      val series = Series(indices, values)
      val bar1 = saddle.barplotHorizontal(series,
        xLabFontSize = Option(RelFontSize(1)),
        color = RedBlue(86, 186))(par
        .xlab("Año Torneo")
        .ylab("freq.")
        .xLabelRotation(-77)
        .xNumTicks(0)
        .main("Goles Torneo"))
      pngToFile(new File("C:\\Users\\josep\\IdeaProjects\\proyectoPF\\Imagenes\\Archivos\\GxT.png"), bar1.build, 400)
    }

    //Generar Imagen Posicion en todos los torneos
    def chartBarQWandM(data: List[Map[String, String]]) = {
      val data4Chart: Map[String, Double] = data
        .map(row => (row("squads_tournament_id"), row("squads_team_id"), row("squads_player_id"), row("squads_position_name"))) // Mapea los datos para obtener una tupla con información relevante de cada jugador.
        .distinct // Eliminar duplicados
        .map(row => row._4 -> (row._1, row._2)) // Mapea los datos para obtener una tupla con la posición del jugador y el identificador del equipo y del torneo.
        .groupBy(_._1) // Agrupa los datos por la posición del jugador.
        .map(row => (row._1, row._2.length.toDouble)) // Mapea los datos para calcular la cantidad de jugadores por posición.
      val indices = Index(data4Chart.map(value => value._1).toArray) // Representan las posiciones de los jugadores.
      val values = Vec(data4Chart.map(value => value._2).toArray) // Representan la cantidad de jugadores por posición.
      val series = Series(indices, values)
      val minValue = data4Chart.values.min // Calcula el valor mínimo de la cantidad de jugadores por posición.
      val maxValue = data4Chart.values.max // Calcula el valor máximo
      val color = RedBlue(minValue, maxValue) // Define el esquema de color para el gráfico, basado en el valor mínimo y máximo de la cantidad de jugadores.
      val bar1 = saddle.barplotHorizontal(series,
        xLabFontSize = Option(RelFontSize(1)),
        color = color
      )(par
        .xlab("Position")
        .ylab("freq.")
        .xLabelRotation(-77)
        .xNumTicks(0)
        .main("Positions In All Tournaments"))
      pngToFile(new File("C:\\Users\\josep\\IdeaProjects\\proyectoPF\\Imagenes\\Archivos\\PxA.png"), bar1.build, 400)
    }

    // Generar Imagen Partidos por Estadio
    def chartBarPlotStadium(xa: Transactor[IO]): Unit = {
      val query = sql"""
        SELECT stadium_name, COUNT(*) as matches_count -- Selecciona el nombre del estadio y cuenta la cantidad de filas para cada estadio
        FROM matches -- De la tabla partidos
        JOIN stadiums ON matches.stadium_id = stadiums.stadium_id -- Une la tabla de partidos con la tabla de estadios basado en el ID del estadio.
        GROUP BY stadium_name -- Agrupa los resultados por el nombre del estadio.
        ORDER BY matches_count DESC
        LIMIT 10
      """.query[(String, Int)]
      val program = query.stream.transact(xa).compile.toList // Ejecuta la consulta y obtiene un programa que produce una secuencia de resultados.
      program.attempt.unsafeRunSync() match {
        case Left(error) =>
          println(s"Error al obtener datos de la base de datos: $error")
        case Right(data) =>
          val indices = Index(data.map(_._1).toArray)
          val values = Vec(data.map(_._2.toDouble).toArray)
          val series = Series(indices, values)
          val bar1 = saddle.barplotHorizontal(series,
            xLabFontSize = Option(RelFontSize(1)),
            color = RedBlue(86, 186))(
            par
              .xlab("Estadio")
              .ylab("Cantidad de partidos")
              .xLabelRotation(-77)
              .main("Partidos por Estadio")
          )
          pngToFile(new File("C:\\Users\\josep\\IdeaProjects\\proyectoPF\\Imagenes\\Base de Datos\\PxE.png"), bar1.build, 400)
      }
    }
    //Generar Imagen cantidad de equipos femeninos y masculinos
    def chartBarTeamsGenre(xa: Transactor[IO]): Unit = {
      val query = sql"""
        SELECT
          SUM(CASE WHEN t.mens_team = 1 THEN 1 ELSE 0 END) as mens_teams_count, -- Suma 1 para cada equipo
          SUM(CASE WHEN t.womens_team = 1 THEN 1 ELSE 0 END) as womens_teams_count
        FROM teams t
      """.query[(Int, Int)]
      val program = query.unique.transact(xa)
      program.attempt.unsafeRunSync() match {
        case Left(error) =>
          println(s"Error al obtener datos de la base de datos: $error")
        case Right((mensTeamsCount, womensTeamsCount)) =>
          val indices = Index(Array("Equipos Masculinos", "Equipos Femeninos"))
          val values = Vec(Array(mensTeamsCount.toDouble, womensTeamsCount.toDouble))
          val series = Series(indices, values)
          val bar2 = saddle.barplotHorizontal(series,
            xLabFontSize = Option(RelFontSize(1)),
            color = RedBlue(86, 186))(
            par
              .xlab("Tipo de Equipo")
              .ylab("Cantidad de equipos")
              .xLabelRotation(-77)
              .main("Equipos Masculinos vs Femeninos")
          )
          pngToFile(new File("C:\\Users\\josep\\IdeaProjects\\proyectoPF\\Imagenes\\Base de Datos\\ExMvF.png"), bar2.build, 400)
      }
    }

    //Generar Imagen cantidad de Torneos por Año
    def chartBarWTournaments(xa: Transactor[IO]): Unit = {
      val query = sql"""
        SELECT year, COUNT(*) as tournaments_count -- Selecciona el año y cuenta el número de torneos para cada año.
        FROM tournaments -- De la tabla de torneos.
        GROUP BY year
        ORDER BY year
      """.query[(Int, Int)]
      val program = query.stream.transact(xa).compile.toList
      program.attempt.unsafeRunSync() match {
        case Left(error) =>
          println(s"Error al obtener datos de la base de datos: $error")
        case Right(data) =>
          val indices = Index(data.map(_._1.toString).toArray)
          val values = Vec(data.map(_._2.toDouble).toArray)
          val series = Series(indices, values)
          val bar3 = saddle.barplotHorizontal(series,
            xLabFontSize = Option(RelFontSize(1)),
            color = RedBlue(86, 186))(
            par
              .xlab("Año del Torneo")
              .ylab("Cantidad de torneos")
              .xLabelRotation(-77)
              .main("Torneos por Año")
          )
          pngToFile(new File("C:\\Users\\josep\\IdeaProjects\\proyectoPF\\Imagenes\\Base de Datos\\TxA.png"), bar3.build, 400)
      }
    }
  }
}
