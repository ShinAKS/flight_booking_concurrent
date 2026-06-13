package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	_ "github.com/lib/pq"
	goredislib "github.com/redis/go-redis/v9"
)

type LockStrategy int

const (
	ForUpdate           LockStrategy = iota
	ForUpdateSkipLocked
	ForUpdateNoWait
	RedisLock
)

func (s LockStrategy) String() string {
	switch s {
	case ForUpdate:
		return "FOR UPDATE"
	case ForUpdateSkipLocked:
		return "FOR UPDATE SKIP LOCKED"
	case ForUpdateNoWait:
		return "FOR UPDATE NOWAIT"
	case RedisLock:
		return "Redis Distributed Lock"
	}
	return "unknown"
}

func (s LockStrategy) SQLClause() string {
	switch s {
	case ForUpdate:
		return "FOR UPDATE"
	case ForUpdateSkipLocked:
		return "FOR UPDATE SKIP LOCKED"
	case ForUpdateNoWait:
		return "FOR UPDATE NOWAIT"
	}
	return ""
}

type BookingResult struct {
	CustomerID int
	SeatID     int
	Err        error
}

// bookSeatPG handles FOR UPDATE / SKIP LOCKED / NOWAIT — all DB-level locking.
func bookSeatPG(db *sql.DB, customerID int, strategy LockStrategy) BookingResult {
	tx, err := db.Begin()
	if err != nil {
		return BookingResult{CustomerID: customerID, Err: err}
	}
	defer tx.Rollback() //nolint

	query := fmt.Sprintf(
		`SELECT seat_id FROM seats WHERE status = 'available' LIMIT 1 %s`,
		strategy.SQLClause(),
	)

	var seatID int
	err = tx.QueryRow(query).Scan(&seatID)
	if err == sql.ErrNoRows {
		tx.Commit() //nolint
		return BookingResult{CustomerID: customerID, SeatID: 0}
	}
	if err != nil {
		return BookingResult{CustomerID: customerID, Err: err}
	}

	_, err = tx.Exec(
		`UPDATE seats SET status = 'booked', customer_id = $1 WHERE seat_id = $2`,
		customerID, seatID,
	)
	if err != nil {
		return BookingResult{CustomerID: customerID, Err: err}
	}

	if err = tx.Commit(); err != nil {
		return BookingResult{CustomerID: customerID, Err: err}
	}

	return BookingResult{CustomerID: customerID, SeatID: seatID}
}

// bookSeatRedis holds a distributed Redis mutex for the entire find+book operation.
// No DB-level row lock is used; correctness is guaranteed by the mutex serializing access.
func bookSeatRedis(db *sql.DB, rs *redsync.Redsync, customerID int) BookingResult {
	mutex := rs.NewMutex("flight-booking-lock",
		redsync.WithExpiry(30*time.Second),
		redsync.WithTries(2000),
		redsync.WithRetryDelay(10*time.Millisecond),
	)

	if err := mutex.LockContext(context.Background()); err != nil {
		return BookingResult{CustomerID: customerID, Err: fmt.Errorf("redis lock: %w", err)}
	}
	defer mutex.Unlock() //nolint

	var seatID int
	err := db.QueryRow(`SELECT seat_id FROM seats WHERE status = 'available' LIMIT 1`).Scan(&seatID)
	if err == sql.ErrNoRows {
		return BookingResult{CustomerID: customerID, SeatID: 0}
	}
	if err != nil {
		return BookingResult{CustomerID: customerID, Err: err}
	}

	_, err = db.Exec(
		`UPDATE seats SET status = 'booked', customer_id = $1 WHERE seat_id = $2`,
		customerID, seatID,
	)
	if err != nil {
		return BookingResult{CustomerID: customerID, Err: err}
	}

	return BookingResult{CustomerID: customerID, SeatID: seatID}
}

func runBookings(db *sql.DB, rs *redsync.Redsync, numCustomers int, strategy LockStrategy) ([]BookingResult, time.Duration) {
	results := make([]BookingResult, numCustomers)
	var wg sync.WaitGroup

	start := time.Now()
	for i := 1; i <= numCustomers; i++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			if strategy == RedisLock {
				results[cid-1] = bookSeatRedis(db, rs, cid)
			} else {
				results[cid-1] = bookSeatPG(db, cid, strategy)
			}
		}(i)
	}
	wg.Wait()

	return results, time.Since(start)
}

func countResults(results []BookingResult) (booked, noSeat, failed int) {
	for _, r := range results {
		switch {
		case r.Err != nil:
			failed++
		case r.SeatID == 0:
			noSeat++
		default:
			booked++
		}
	}
	return
}

func resetSeats(db *sql.DB) error {
	_, err := db.Exec(`UPDATE seats SET status = 'available', customer_id = NULL`)
	return err
}

func printGrid(db *sql.DB) {
	rows, err := db.Query(`SELECT seat_id, status, customer_id FROM seats ORDER BY seat_id`)
	if err != nil {
		fmt.Println("Error reading seats:", err)
		return
	}
	defer rows.Close()

	type SeatRow struct {
		SeatID     int
		Status     string
		CustomerID sql.NullInt64
	}

	seats := make([]SeatRow, 0, 100)
	for rows.Next() {
		var s SeatRow
		if err := rows.Scan(&s.SeatID, &s.Status, &s.CustomerID); err != nil {
			fmt.Println("scan error:", err)
			return
		}
		seats = append(seats, s)
	}

	fmt.Println()
	fmt.Println("=== Seat Map (5 rows × 20 cols) ===")
	fmt.Println()

	fmt.Print("     ")
	for col := 1; col <= 20; col++ {
		fmt.Printf(" %3d", col)
	}
	fmt.Println()
	fmt.Print("     ")
	for col := 1; col <= 20; col++ {
		fmt.Print(" ---")
	}
	fmt.Println()

	for row := 0; row < 5; row++ {
		fmt.Printf("R%02d |", row+1)
		for col := 0; col < 20; col++ {
			s := seats[row*20+col]
			if s.Status == "booked" && s.CustomerID.Valid {
				fmt.Printf(" %3d", s.CustomerID.Int64)
			} else {
				fmt.Printf("   X")
			}
		}
		fmt.Println()
	}
	fmt.Println()
}

func chooseStrategy() LockStrategy {
	fmt.Println("Choose locking strategy:")
	fmt.Println("  1) FOR UPDATE")
	fmt.Println("  2) FOR UPDATE SKIP LOCKED")
	fmt.Println("  3) FOR UPDATE NOWAIT")
	fmt.Println("  4) Redis Distributed Lock")
	fmt.Print("Enter choice [1-4]: ")

	var choice int
	fmt.Scan(&choice)

	switch choice {
	case 1:
		return ForUpdate
	case 2:
		return ForUpdateSkipLocked
	case 3:
		return ForUpdateNoWait
	case 4:
		return RedisLock
	default:
		fmt.Println("Invalid choice, defaulting to FOR UPDATE")
		return ForUpdate
	}
}

type cell struct {
	booked  int
	elapsed time.Duration
}

func runBench(db *sql.DB, rdb *goredislib.Client, rs *redsync.Redsync, counts []int) {
	strategies := []LockStrategy{ForUpdate, ForUpdateSkipLocked, ForUpdateNoWait, RedisLock}

	table := make([][]cell, len(strategies))
	for i := range table {
		table[i] = make([]cell, len(counts))
	}

	for si, strat := range strategies {
		fmt.Printf("\nRunning: %s\n", strat)
		for ci, n := range counts {
			if err := resetSeats(db); err != nil {
				fmt.Println("reset error:", err)
				return
			}
			if strat == RedisLock {
				clearRedisLock(rdb)
			}
			results, elapsed := runBookings(db, rs, n, strat)
			booked, _, _ := countResults(results)
			table[si][ci] = cell{booked, elapsed}
			fmt.Printf("  customers=%-4d  booked=%-3d  time=%s\n", n, booked, elapsed.Round(time.Millisecond))
		}
	}

	fmt.Println()
	fmt.Println("=== Benchmark Summary ===")
	fmt.Println()
	header := fmt.Sprintf("%-26s", "Strategy")
	for _, n := range counts {
		header += fmt.Sprintf("  %6d", n)
	}
	fmt.Println(header)
	fmt.Println(repeatStr("-", len(header)+4))

	bookedLabels := []string{"Booked (FU)", "Booked (SL)", "Booked (NW)", "Booked (Redis)"}
	for si := range strategies {
		row := fmt.Sprintf("%-26s", bookedLabels[si]+" seats")
		for ci := range counts {
			row += fmt.Sprintf("  %6d", table[si][ci].booked)
		}
		fmt.Println(row)
	}

	fmt.Println()
	timeLabels := []string{"Time (FU)", "Time (SL)", "Time (NW)", "Time (Redis)"}
	for si := range strategies {
		row := fmt.Sprintf("%-26s", timeLabels[si])
		for ci := range counts {
			row += fmt.Sprintf("  %5dms", table[si][ci].elapsed.Milliseconds())
		}
		fmt.Println(row)
	}

	fmt.Println()
	fmt.Println("=== Latency Curve (ms) ===")
	fmt.Println()
	printASCIICurve(counts, table)
}

func printASCIICurve(counts []int, table [][]cell) {
	const height = 20
	const width = 60

	var maxMs int64
	for _, row := range table {
		for _, c := range row {
			if ms := c.elapsed.Milliseconds(); ms > maxMs {
				maxMs = ms
			}
		}
	}
	if maxMs == 0 {
		maxMs = 1
	}

	symbols := []byte{'#', '*', '+', '@'}
	names := []string{"FOR UPDATE (#)", "SKIP LOCKED (*)", "NOWAIT (+)", "Redis Lock (@)"}

	grid := make([][]byte, height)
	for i := range grid {
		grid[i] = make([]byte, width)
		for j := range grid[i] {
			grid[i][j] = ' '
		}
	}

	colWidth := width / len(counts)
	for si, row := range table {
		for ci, c := range row {
			ms := c.elapsed.Milliseconds()
			x := ci*colWidth + colWidth/2
			y := height - 1 - int(float64(ms)/float64(maxMs)*float64(height-1))
			if y < 0 {
				y = 0
			}
			if x < width {
				grid[y][x] = symbols[si]
			}
		}
	}

	for y, row := range grid {
		if y == 0 {
			fmt.Printf("%5dms |%s\n", maxMs, string(row))
		} else if y == height/2 {
			fmt.Printf("%5dms |%s\n", maxMs/2, string(row))
		} else if y == height-1 {
			fmt.Printf("%5dms |%s\n", int64(0), string(row))
		} else {
			fmt.Printf("       |%s\n", string(row))
		}
	}

	fmt.Printf("       +%s\n", repeatStr("-", width))
	xLabel := "        "
	for _, n := range counts {
		xLabel += fmt.Sprintf("%6d", n)
	}
	fmt.Println(xLabel)
	fmt.Println("                           customers →")
	fmt.Println()
	for _, name := range names {
		fmt.Printf("  %s\n", name)
	}
}

func repeatStr(s string, n int) string {
	out := make([]byte, n*len(s))
	for i := range out {
		out[i] = s[0]
	}
	return string(out)
}

func newRedisClient() *goredislib.Client {
	host := getenv("REDIS_HOST", "localhost")
	port := getenv("REDIS_PORT", "6379")
	return goredislib.NewClient(&goredislib.Options{
		Addr: fmt.Sprintf("%s:%s", host, port),
	})
}

func newRedisSync(client *goredislib.Client) *redsync.Redsync {
	return redsync.New(goredis.NewPool(client))
}

// clearRedisLock deletes any stale lock key left by a previous crashed run.
func clearRedisLock(client *goredislib.Client) {
	client.Del(context.Background(), "flight-booking-lock")
}

func dsn() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		getenv("DB_HOST", "localhost"),
		getenv("DB_PORT", "5432"),
		getenv("DB_USER", "postgres"),
		getenv("DB_PASS", "postgres"),
		getenv("DB_NAME", "flight_booking"),
	)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	bench := flag.Bool("bench", false, "run benchmark across all strategies and customer counts")
	flag.Parse()

	db, err := sql.Open("postgres", dsn())
	if err != nil {
		fmt.Println("Failed to open DB:", err)
		os.Exit(1)
	}
	defer db.Close()

	db.SetMaxOpenConns(550)
	db.SetMaxIdleConns(550)

	if err := db.Ping(); err != nil {
		fmt.Println("Cannot connect to DB:", err)
		fmt.Println("Set DB_HOST / DB_PORT / DB_USER / DB_PASS / DB_NAME env vars if needed.")
		os.Exit(1)
	}

	rdb := newRedisClient()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		fmt.Println("Cannot connect to Redis:", err)
		fmt.Println("Set REDIS_HOST / REDIS_PORT env vars if needed, or run: docker compose up -d")
		os.Exit(1)
	}
	rs := newRedisSync(rdb)

	if *bench {
		runBench(db, rdb, rs, []int{50, 100, 200, 300, 400, 500})
		return
	}

	strategy := chooseStrategy()
	fmt.Printf("\nUsing strategy: %s\n", strategy)

	if err := resetSeats(db); err != nil {
		fmt.Println("Failed to reset seats:", err)
		os.Exit(1)
	}
	if strategy == RedisLock {
		clearRedisLock(rdb)
	}

	fmt.Printf("Firing 100 concurrent booking requests...\n\n")
	results, elapsed := runBookings(db, rs, 100, strategy)
	booked, noSeat, failed := countResults(results)
	fmt.Printf("Booked: %d | No seat available: %d | Errors: %d | Time: %s\n",
		booked, noSeat, failed, elapsed.Round(time.Millisecond))
	if failed > 0 {
		for _, r := range results {
			if r.Err != nil {
				fmt.Printf("  sample error (customer %d): %v\n", r.CustomerID, r.Err)
				break
			}
		}
	}
	printGrid(db)
}
