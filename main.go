package main

import (
	"fmt"
	"os"
	"sync"

	"github.com/gocarina/gocsv"
)

// Player ...
type Player struct {
	ID                      uint64     `json:"id" csv:"ID"`
	Name                    string     `json:"name" csv:"Name"`
	Age                     int        `json:"age" csv:"Age"`
	Photo                   string     `json:"photo" csv:"Photo"`
	Nationality             string     `json:"nation" csv:"Nationality"`
	Flag                    string     `json:"flag" csv:"Flag"`
	Overall                 int        `json:"overall" csv:"Overall"`
	Potential               int        `json:"potential" csv:"Potential"`
	Club                    string     `json:"club" csv:"Club"`
	ClubLogo                string     `json:"club_logo" csv:"Club Logo"`
	Value                   string     `json:"value" csv:"Value"`
	Wage                    string     `json:"wage" csv:"Wage"`
	Special                 int        `json:"special" csv:"Special"`
	PreferredFoot           string     `json:"preferred_foot" csv:"Preferred Foot"`
	InternationalReputation int        `json:"international_reputation" csv:"International Reputation"`
	WeakFoot                int        `json:"weak_foot" csv:"Weak Foot"`
	SkillMoves              int        `json:"skill_moves" csv:"Skill Moves"`
	WorkRate                string     `json:"work_rate" csv:"Work Rate"`
	BodyType                string     `json:"body_type" csv:"Body Type"`
	RealFace                string     `json:"real_face" csv:"Real Face"`
	Position                string     `json:"position" csv:"Position"`
	JerseyNumber            int        `json:"jersey_number" csv:"Jersey Number"`
	Joined                  string     `json:"joined" csv:"Joined"`
	LoanedFrom              string     `json:"loaned_from" csv:"Loaned From"`
	ContractValidUntil      string     `json:"contract_valid_until" csv:"Contract Valid Until"`
	Height                  string     `json:"height" csv:"Height"`
	Weight                  string     `json:"weight" csv:"Weight"`
	Stat                    PlayerStat `json:"player_stat" csv:"Stat"`
	ReleaseClause           string     `json:"release_clause" csv:"Release Clause"`
}

// PlayerStat ...
type PlayerStat struct {
	LS              string `json:"ls" csv:"LS"`
	ST              string `json:"st" csv:"ST"`
	RS              string `json:"rs" csv:"RS"`
	LW              string `json:"lw" csv:"LW"`
	LF              string `json:"lf" csv:"LF"`
	CF              string `json:"cf" csv:"CF"`
	RF              string `json:"rf" csv:"RF"`
	RW              string `json:"rw" csv:"RW"`
	LAM             string `json:"lam" csv:"LAM"`
	CAM             string `json:"cam" csv:"CAM"`
	RAM             string `json:"ram" csv:"RAM"`
	LM              string `json:"lm" csv:"LM"`
	LCM             string `json:"lcm" csv:"LCM"`
	CM              string `json:"cm" csv:"CM"`
	RCM             string `json:"rcm" csv:"RCM"`
	RM              string `json:"rm" csv:"RM"`
	LWB             string `json:"lwb" csv:"LWB"`
	LDM             string `json:"ldm" csv:"LDM"`
	CDM             string `json:"cdm" csv:"CDM"`
	RDM             string `json:"rdm" csv:"RDM"`
	RWB             string `json:"rwb" csv:"RWB"`
	LB              string `json:"lb" csv:"LB"`
	LCB             string `json:"lcb" csv:"LCB"`
	CB              string `json:"cb" csv:"CB"`
	RCB             string `json:"rcb" csv:"RCB"`
	RB              string `json:"rb" csv:"RB"`
	Crossing        int    `json:"crossing" csv:"Crossing"`
	Finishing       int    `json:"finishing" csv:"Finishing"`
	HeadingAccuracy int    `json:"heading_accuracy" csv:"HeadingAccuracy"`
	ShortPassing    int    `json:"short_passing" csv:"ShortPassing"`
	Volleys         int    `json:"volleys" csv:"Volleys"`
	Dribbling       int    `json:"dribbling" csv:"Dribbling"`
	Curve           int    `json:"curve" csv:"Curve"`
	FKAccuracy      int    `json:"fk_accuracy" csv:"FKAccuracy"`
	LongPassing     int    `json:"long_passing" csv:"LongPassing"`
	BallControl     int    `json:"ball_control" csv:"BallControl"`
	Acceleration    int    `json:"acceleration" csv:"Acceleration"`
	SprintSpeed     int    `json:"sprint_speed" csv:"SprintSpeed"`
	Agility         int    `json:"agility" csv:"Agility"`
	Reactions       int    `json:"reactions" csv:"Reactions"`
	Balance         int    `json:"balance" csv:"Balance"`
	ShotPower       int    `json:"shot_power" csv:"ShotPower"`
	Jumping         int    `json:"jumping" csv:"Jumping"`
	Stamina         int    `json:"stamina" csv:"Stamina"`
	Strength        int    `json:"strength" csv:"Strength"`
	LongShots       int    `json:"long_shots" csv:"LongShots"`
	Aggression      int    `json:"aggression" csv:"Aggression"`
	Interceptions   int    `json:"interceptions" csv:"Interceptions"`
	Positioning     int    `json:"positioning" csv:"Positioning"`
	Vision          int    `json:"vision" csv:"Vision"`
	Penalties       int    `json:"penalties" csv:"Penalties"`
	Composure       int    `json:"composure" csv:"Composure"`
	Marking         int    `json:"marking" csv:"Marking"`
	StandingTackle  int    `json:"standing_tackle" csv:"StandingTackle"`
	GKDiving        int    `json:"gk_diving" csv:"GKDiving"`
	GKKicking       int    `json:"gk_kicking" csv:"GKKicking"`
	GKPositioning   int    `json:"gk_positioning" csv:"GKPositioning"`
	GKReflexes      int    `json:"gk_reflexes" csv:"GKReflexes"`
}

func log(i ...interface{}) {
	fmt.Println(i...)
}

func logVerbose(i ...interface{}) {
	fmt.Printf("%+v\n", i...)
}

const (
	under20 string = "Under 20-years old"
	under30 string = "Under 30-years old"
	under40 string = "Under 40-years old"
	retired string = "Should retire"
)

type ageCount struct {
	age   int
	count int
}

type catCount struct {
	category string
	count    int
}

func mapper(p *Player, c chan<- ageCount) {
	c <- ageCount{p.Age, 1}
}

func sorter(from <-chan ageCount, to chan<- catCount) {
	for ac := range from {
		switch {
		case ac.age < 20:
			to <- catCount{under20, ac.count}
		case ac.age < 30:
			to <- catCount{under30, ac.count}
		case ac.age < 40:
			to <- catCount{under40, ac.count}
		default:
			to <- catCount{retired, ac.count}
		}
	}

	close(to)
}

func reducer(from <-chan catCount, out chan<- map[string]int) {
	output := make(map[string]int)
	for e := range from {
		output[e.category] += e.count
	}

	out <- output
	close(out)
}

func main() {
	file, err := os.Open("data.csv")
	if err != nil {
		panic(err)
	}

	defer file.Close()

	players := []*Player{}

	if err := gocsv.UnmarshalFile(file, &players); err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(players))

	// Mapper: process all input and extract the expected values
	mapChan := make(chan ageCount, len(players))
	for _, player := range players {
		go func(p *Player) {
			mapper(p, mapChan)
			wg.Done()
		}(player)
	}

	categoryChan := make(chan catCount, len(players))
	// Shuffle and sort: get the processed inputs and sort them into the predefined patterns
	go sorter(mapChan, categoryChan)

	result := make(chan map[string]int)
	// Reducer: get the data that classified by patterns and do statistics on this data such as: min, max, sum...
	go reducer(categoryChan, result)

	wg.Wait()
	close(mapChan)

	output := <-result
	for k, v := range output {
		log(k, "->", v, "players")
	}
}
