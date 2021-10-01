open System.Data
#r "nuget: Taskbuilder.fs"
#r "nuget: Microsoft.Data.SqlClient"

open Microsoft.Data.SqlClient
open System

type PokemonTrainerId =
    | PokemonTrainerId of Guid

    member this.Serialise () = 
        match this with 
        | PokemonTrainerId id -> id 
    
    static member Deserialise id = PokemonTrainerId id

type PokeIndex =
    | PokeIndex of int

    member this.Serialise () = 
        match this with 
        | PokeIndex id -> id

    static member Deserialise index = PokeIndex index


type PokemonType =
    | Rock
    | Grass
    | Poison
    | Fire
    | Psychic
    | Ghost
    | Ice
    
    member this.Id = 
        match this with  
        | Rock -> 0
        | Grass -> 1
        | Poison -> 2
        | Fire -> 3
        | Psychic -> 4
        | Ghost -> 5
        | Ice -> 6

    member this.Serialise () = 
        match this with 
        | Rock -> "Rock"
        | Grass -> "Grass"
        | Poison -> "Poison"
        | Fire -> "Fire"
        | Psychic -> "Psychic"
        | Ghost -> "Ghost"
        | Ice -> "Ice"
    
    static member Deserialise = function 
        | "Rock" -> Rock
        | "Grass" -> Grass
        | "Poison" -> Poison
        | "Fire" -> Fire
        | "Psychic" -> Psychic
        | "Ghost" -> Ghost
        | "Ice" -> Ice
        | x -> failwithf $"Invalid PokemonType: {x}"

type Pokemon =
    { PokeIndex : PokeIndex
      Name : string
      Level: int
      PokemonTypes: PokemonType list
      EvolutionName: string option }

type Record =
    { Wins: uint; Losses: uint }

type PokemonTrainer =
    { Id : PokemonTrainerId
      Name : string
      Pokemon : Pokemon list
      Record: Record }

let getConnection () = new SqlConnection """Server=tcp:localhost,1433;Initial Catalog=pokemon;Persist Security Info=True;User ID=sa;Password=Str0ngPa55w0rd!;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=True;Connection Timeout=30;"""

let charmander =
    { PokeIndex = PokeIndex 7
      Name = "Charmander"
      Level = 80
      PokemonTypes = [ Fire ]
      EvolutionName = Some "Charmeleon" }

let venusaur =
    { PokeIndex = PokeIndex 2
      Name = "Venusaur"
      Level = 65
      PokemonTypes = [ Grass; Poison ]
      EvolutionName = None }

let ashKetchum =
    { Id = PokemonTrainerId (Guid.NewGuid())
      Name = "Ash Ketchum"
      Pokemon = [ charmander; venusaur ]
      Record = { Wins = 120u; Losses = 10u } }

module PokemonTypes = 
    let seedAllPokemonTypes () =
        use conn = getConnection ()
        let sqlQuery = "INSERT INTO PokemonTypes (PokemonTypeId, PokemonTypeName) VALUES (@PokemonTypeId, @PokemonTypeName)"

        conn.Open ()
        
        let types = [ Rock; Grass; Poison; Fire; Psychic; Ghost; Ice ]

        for pokemonType in types do 
            use cmd = new SqlCommand (sqlQuery, conn)
            cmd.Parameters.AddWithValue("@PokemonTypeId", pokemonType.Id) |> ignore
            cmd.Parameters.AddWithValue("@PokemonTypeName", pokemonType.Serialise ()) |> ignore 
            cmd.ExecuteNonQuery () |> ignore


module Pokemon =

    let addPokemon (trainerId: PokemonTrainerId) (pokemon: Pokemon) =
        use conn = getConnection ()
        let pokemonSqlQuery = "INSERT INTO Pokemon (PokeIndex, Name, EvolutionName, PokemonTrainerId, Level) VALUES (@PokeIndex, @Name, @EvolutionName, @PokemonTrainerId, @Level)"

        use cmd = new SqlCommand (pokemonSqlQuery, conn)

        conn.Open ()

        let evolutionName = 
            match pokemon.EvolutionName with
            | Some name -> box name
            | None -> box DBNull.Value

        let pokeIndex = pokemon.PokeIndex.Serialise ()

        cmd.Parameters.AddWithValue ("@PokeIndex", pokeIndex) |> ignore
        cmd.Parameters.AddWithValue ("@Name", pokemon.Name) |> ignore
        cmd.Parameters.AddWithValue ("@EvolutionName", evolutionName) |> ignore
        cmd.Parameters.AddWithValue ("@PokemonTrainerId", trainerId.Serialise ()) |> ignore
        cmd.Parameters.AddWithValue ("@Level", pokemon.Level) |> ignore

        cmd.ExecuteNonQuery () |> ignore

        let pokemonTypeSqlQuery = "INSERT INTO PokemonType_Pokemon (PokemonTypeId, PokeIndex) VALUES (@PokemonTypeId, @PokeIndex)"

        pokemon.PokemonTypes
        |> List.iter (fun pokemonType ->
            use pokemonTypeCmd = new SqlCommand (pokemonTypeSqlQuery, conn)
            pokemonTypeCmd.Parameters.AddWithValue("@PokemonTypeId", pokemonType.Id) |> ignore
            pokemonTypeCmd.Parameters.AddWithValue("@PokeIndex", pokeIndex) |> ignore 
            pokemonTypeCmd.ExecuteNonQuery () |> ignore )
    
    let getTrainersPokemon (trainerId: PokemonTrainerId) = 
        use conn = getConnection ()
        let sqlQuery = 
            "
            SELECT Pokemon.PokeIndex, Name, EvolutionName, Level, PokemonTypeName 
            FROM Pokemon 
            JOIN PokemonType_Pokemon ON Pokemon.PokeIndex = PokemonType_Pokemon.PokeIndex 
            JOIN PokemonTypes ON PokemonType_Pokemon.PokemonTypeId = PokemonTypes.PokemonTypeId 
            WHERE PokemonTrainerId = @PokemonTrainerId
            "

        use cmd = new SqlCommand (sqlQuery, conn)

        conn.Open ()

        cmd.Parameters.AddWithValue ("@PokemonTrainerId", trainerId.Serialise ()) |> ignore

        let reader = cmd.ExecuteReader ()

        seq {
            while reader.Read () do
            let pokeIndex = 
                "PokeIndex"
                |> reader.GetOrdinal
                |> reader.GetInt32
                |> PokeIndex 
            
            let name =
                "Name"
                |> reader.GetOrdinal
                |> reader.GetString
            
            let evolutionName =
                let canEvolve = 
                    "EvolutionName"
                    |> reader.GetOrdinal
                    |> reader.IsDBNull
                    |> not 

                if canEvolve then
                    "EvolutionName"
                    |> reader.GetOrdinal
                    |> reader.GetString
                    |> Some 
                else
                    None
                
            let level =
                "Level"
                |> reader.GetOrdinal
                |> reader.GetInt32
            

            let pokemonType = 
                "PokemonTypeName"
                |> reader.GetOrdinal
                |> reader.GetString
                |> PokemonType.Deserialise

            { PokeIndex = pokeIndex
              Name = name
              Level = level
              PokemonTypes = [ pokemonType ] 
              EvolutionName = evolutionName }
        }
        |> Seq.groupBy (fun pokemon -> pokemon.PokeIndex)
        |> Seq.map (fun (_, pokemon) -> 
            pokemon 
            |> Seq.reduce (fun acc pokemon -> 
                { acc with PokemonTypes = acc.PokemonTypes @ pokemon.PokemonTypes }
                )
            )
        |> Seq.toList


module PokemonTrainer = 
    let addPokemonTrainer trainer = 
        use conn = getConnection ()
        let pokemonTrainerSqlQuery = "INSERT INTO PokemonTrainer (Id, Name, Wins, Losses) VALUES (@Id, @Name, @Wins, @Losses)"

        use cmd = new SqlCommand (pokemonTrainerSqlQuery, conn)

        conn.Open ()

        cmd.Parameters.AddWithValue ("@Id", (trainer.Id.Serialise ())) |> ignore
        cmd.Parameters.AddWithValue ("@Name", trainer.Name) |> ignore
        cmd.Parameters.AddWithValue ("@Wins", int trainer.Record.Wins) |> ignore
        cmd.Parameters.AddWithValue ("@Losses", int trainer.Record.Losses) |> ignore
        cmd.ExecuteNonQuery () |> ignore

        trainer.Pokemon |> List.iter (Pokemon.addPokemon trainer.Id)

    let getPokemonTrainer (trainerId: PokemonTrainerId) = 
        use conn = getConnection ()
        let sqlQuery = 
            "
            SELECT Id, PokemonTrainer.Name as TrainerName, Wins, Losses, Pokemon.PokeIndex, Pokemon.Name as PokemonName, EvolutionName, Level, PokemonTypeName 
            FROM PokemonTrainer 
            LEFT JOIN Pokemon ON PokemonTrainer.Id = Pokemon.PokemonTrainerId
            JOIN PokemonType_Pokemon ON Pokemon.PokeIndex = PokemonType_Pokemon.PokeIndex
            JOIN PokemonTypes ON PokemonType_Pokemon.PokemonTypeId = PokemonTypes.PokemonTypeId
            WHERE Id = @Id
            "

        use cmd = new SqlCommand (sqlQuery, conn)

        conn.Open ()

        cmd.Parameters.AddWithValue ("@Id", trainerId.Serialise ()) |> ignore

        let reader = cmd.ExecuteReader ()

        seq {
            while reader.Read () do  
            let id = 
                "Id"
                |> reader.GetOrdinal
                |> reader.GetGuid
                |> PokemonTrainerId 

            let name =
                "TrainerName"
                |> reader.GetOrdinal
                |> reader.GetString

            let wins =
                "Wins"
                |> reader.GetOrdinal
                |> reader.GetInt32
                |> uint32

            let losses =
                "Losses"
                |> reader.GetOrdinal
                |> reader.GetInt32
                |> uint32

            let record = 
                { Wins = wins 
                  Losses = losses }
            
            let pokeIndex = 
                "PokeIndex"
                |> reader.GetOrdinal
                |> reader.GetInt32

            let pokemonName = 
                "PokemonName"
                |> reader.GetOrdinal
                |> reader.GetString 

            let level = 
                "Level"
                |> reader.GetOrdinal
                |> reader.GetInt32
            
            let pokemonType = 
                "PokemonTypeName"
                |> reader.GetOrdinal
                |> reader.GetString 
                |> PokemonType.Deserialise

            let evolutionName =
                let canEvolve = 
                    "EvolutionName"
                    |> reader.GetOrdinal
                    |> reader.IsDBNull
                    |> not 

                if canEvolve then
                    "EvolutionName"
                    |> reader.GetOrdinal
                    |> reader.GetString
                    |> Some 
                else
                    None
            
            let pokemon = 
                { PokeIndex = PokeIndex pokeIndex
                  Name = pokemonName 
                  Level = level
                  PokemonTypes = [ pokemonType ]
                  EvolutionName = evolutionName}

            { Id = id
              Name = name
              Record = record
              Pokemon = [ pokemon ] }
        }
        |> Seq.reduce (fun acc trainer -> 
            { acc with 
                Pokemon = 
                acc.Pokemon 
                |> (@) trainer.Pokemon
                |> Seq.groupBy (fun pokemon -> pokemon.PokeIndex)
                |> Seq.map (fun (_, pokemon) -> 
                    pokemon 
                    |> Seq.reduce (fun acc pokemon -> 
                        { acc with PokemonTypes = acc.PokemonTypes @ pokemon.PokemonTypes }
                        )
                    )
                |> Seq.toList }
            )
