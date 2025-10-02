package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	_ "github.com/nakagami/firebirdsql"
)

// Config структура для хранения конфигурации
type Config struct {
	FirebirdUser     string
	FirebirdPassword string
	FirebirdHost     string
	FirebirdPort     string
	FirebirdDB       string
	FirebirdCharset  string
	PostgresHost     string
	PostgresPort     string
	PostgresUser     string
	PostgresPassword string
	PostgresDB       string
	PostgresSSLMode  string
}

// StaffCard структура для данных сотрудника и карты
type StaffCard struct {
	IDStaff    int64   `json:"id_staff"`
	Identifier string  `json:"identifier"`
	LastName   *string `json:"last_name"`
	FirstName  *string `json:"first_name"`
	MiddleName *string `json:"middle_name"`
	Status     *string `json:"status"`
	Info       *string `json:"info"`
}

// APIResponse структура для ответов API
type APIResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

var (
	config Config
	tmpl   *template.Template
)

func init() {
	// Загрузка .env файла
	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: .env file not found: %v", err)
	}

	// Инициализация конфигурации
	config = Config{
		FirebirdUser:     getEnv("FIREBIRD_USER", "sysdba"),
		FirebirdPassword: getEnv("FIREBIRD_PASSWORD", "masterkey"),
		FirebirdHost:     getEnv("FIREBIRD_HOST", "localhost"),
		FirebirdPort:     getEnv("FIREBIRD_PORT", "3050"),
		FirebirdDB:       getEnv("FIREBIRD_DB", ""),
		FirebirdCharset:  getEnv("FIREBIRD_charset", "UTF8"),
		PostgresHost:     getEnv("POSTGRES_HOST", "localhost"),
		PostgresPort:     getEnv("POSTGRES_PORT", "5432"),
		PostgresUser:     getEnv("POSTGRES_USER", "postgres"),
		PostgresPassword: getEnv("POSTGRES_PASSWORD", ""),
		PostgresDB:       getEnv("POSTGRES_DB", "cards_service"),
		PostgresSSLMode:  getEnv("POSTGRES_SSLMODE", "disable"),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// returnJSONError возвращает ошибку в формате JSON
func returnJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(APIResponse{
		Success: false,
		Error:   message,
	})
}

// returnJSONSuccess возвращает успешный ответ в формате JSON
func returnJSONSuccess(w http.ResponseWriter, data interface{}, message string) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Message: message,
		Data:    data,
	})
}

// checkFirebirdConnection проверяет подключение к Firebird
func checkFirebirdConnection() error {
	db, err := connectFirebird()
	if err != nil {
		return fmt.Errorf("failed to connect to Firebird: %v", err)
	}
	defer db.Close()

	// Проверяем подключение с простым запросом
	var result int
	err = db.QueryRow("SELECT 1 FROM RDB$DATABASE").Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to query Firebird: %v", err)
	}

	// Проверяем существование таблиц
	tables := []string{"STAFF", "STAFF_CARDS"}
	for _, table := range tables {
		var tableExists int
		query := fmt.Sprintf("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = '%s'", strings.ToUpper(table))
		err = db.QueryRow(query).Scan(&tableExists)
		if err != nil {
			return fmt.Errorf("failed to check table %s: %v", table, err)
		}
		if tableExists == 0 {
			return fmt.Errorf("table %s does not exist in Firebird database", table)
		}
	}

	log.Printf("✅ Firebird connection successful - connected to %s", config.FirebirdDB)
	return nil
}

// checkPostgresConnection проверяет подключение к PostgreSQL
func checkPostgresConnection() error {
	db, err := connectPostgres()
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Проверяем подключение с простым запросом
	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to query PostgreSQL: %v", err)
	}

	// Проверяем существование базы данных
	var dbExists bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", config.PostgresDB).Scan(&dbExists)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %v", err)
	}

	if !dbExists {
		log.Printf("⚠️ PostgreSQL database '%s' does not exist, it will be created on first connection", config.PostgresDB)
	} else {
		log.Printf("✅ PostgreSQL connection successful - connected to database '%s'", config.PostgresDB)
	}

	return nil
}

func connectFirebird() (*sql.DB, error) {
	connStr := fmt.Sprintf("%s:%s@%s:%s/%s?charset=%s",
		config.FirebirdUser,
		config.FirebirdPassword,
		config.FirebirdHost,
		config.FirebirdPort,
		config.FirebirdDB,
		config.FirebirdCharset,
	)
	log.Printf("Connecting to Firebird: %s@%s:%s/%s",
		config.FirebirdUser, config.FirebirdHost, config.FirebirdPort, config.FirebirdDB)

	db, err := sql.Open("firebirdsql", connStr)
	if err != nil {
		log.Printf("Firebird connection error: %v", err)
		return nil, err
	}

	// Проверяем подключение
	if err := db.Ping(); err != nil {
		log.Printf("Firebird ping error: %v", err)
		return nil, err
	}

	log.Printf("✅ Firebird connection established")
	return db, nil
}

func connectPostgres() (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		config.PostgresHost,
		config.PostgresPort,
		config.PostgresUser,
		config.PostgresPassword,
		config.PostgresDB,
		config.PostgresSSLMode,
	)
	log.Printf("Connecting to PostgreSQL: %s@%s:%s/%s",
		config.PostgresUser, config.PostgresHost, config.PostgresPort, config.PostgresDB)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("PostgreSQL connection error: %v", err)
		return nil, err
	}

	// Проверяем подключение
	if err := db.Ping(); err != nil {
		log.Printf("PostgreSQL ping error: %v", err)
		return nil, err
	}

	log.Printf("✅ PostgreSQL connection established")
	return db, nil
}

func initPostgresTable(db *sql.DB) error {
	// Проверяем существование таблицы
	var tableExists bool
	err := db.QueryRow(`
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = 'staff_cards'
		)
	`).Scan(&tableExists)

	if err != nil {
		return fmt.Errorf("error checking table existence: %v", err)
	}

	if tableExists {
		// Проверяем структуру таблицы
		var columns []string
		rows, err := db.Query(`
			SELECT column_name 
			FROM information_schema.columns 
			WHERE table_name = 'staff_cards'
		`)
		if err != nil {
			return fmt.Errorf("error checking table structure: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var col string
			if err := rows.Scan(&col); err != nil {
				return fmt.Errorf("error scanning column: %v", err)
			}
			columns = append(columns, col)
		}

		requiredColumns := map[string]bool{
			"id_staff": true, "identifier": true, "last_name": true,
			"first_name": true, "middle_name": true, "status": true,
			"info": true, "updated_at": true,
		}

		hasAllColumns := true
		for col := range requiredColumns {
			found := false
			for _, c := range columns {
				if c == col {
					found = true
					break
				}
			}
			if !found {
				hasAllColumns = false
				break
			}
		}

		if !hasAllColumns {
			// Переименовываем старую таблицу
			newName := fmt.Sprintf("staff_cards_old_%s", time.Now().Format("20060102_150405"))
			_, err := db.Exec(fmt.Sprintf("ALTER TABLE staff_cards RENAME TO %s", newName))
			if err != nil {
				return fmt.Errorf("error renaming table: %v", err)
			}
			log.Printf("📁 Old table renamed to %s", newName)
			tableExists = false
		}
	}

	if !tableExists {
		// Создаем новую таблицу с полем updated_at
		_, err := db.Exec(`
			CREATE TABLE staff_cards (
				id_staff BIGINT,
				identifier TEXT,
				last_name VARCHAR(255),
				first_name VARCHAR(255),
				middle_name VARCHAR(255),
				status VARCHAR(50),
				info VARCHAR(50),
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			)
		`)
		if err != nil {
			return fmt.Errorf("error creating table: %v", err)
		}
		log.Printf("✅ Created new table 'staff_cards' with updated_at field")
	} else {
		log.Printf("✅ Table 'staff_cards' already exists with correct structure")
	}

	return nil
}

// updateHandler обрабатывает запрос на обновление данных из Firebird в PostgreSQL
func updateHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("🔄 Starting data update process...")

	// Разрешаем GET и POST запросы
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		returnJSONError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Подключаемся к Firebird
	fbDB, err := connectFirebird()
	if err != nil {
		log.Printf("❌ Firebird connection failed: %v", err)
		returnJSONError(w, fmt.Sprintf("Firebird connection error: %v", err), http.StatusInternalServerError)
		return
	}
	defer fbDB.Close()

	// Получаем данные из Firebird
	log.Println("📥 Fetching data from Firebird...")
	query := `
		SELECT s.LAST_NAME, s.FIRST_NAME, s.MIDDLE_NAME, s.ID_STAFF, sc.IDENTIFIER
		FROM STAFF s
		JOIN STAFF_CARDS sc ON s.ID_STAFF = sc.STAFF_ID
	`
	rows, err := fbDB.Query(query)
	if err != nil {
		log.Printf("❌ Firebird query failed: %v", err)
		returnJSONError(w, fmt.Sprintf("Firebird query error: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var staffCards []StaffCard
	count := 0
	for rows.Next() {
		var sc StaffCard
		var lastName, firstName, middleName sql.NullString

		err := rows.Scan(&lastName, &firstName, &middleName, &sc.IDStaff, &sc.Identifier)
		if err != nil {
			log.Printf("❌ Error scanning row: %v", err)
			returnJSONError(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
			return
		}

		if lastName.Valid {
			sc.LastName = &lastName.String
		}
		if firstName.Valid {
			sc.FirstName = &firstName.String
		}
		if middleName.Valid {
			sc.MiddleName = &middleName.String
		}

		staffCards = append(staffCards, sc)
		count++

		// Логируем прогресс каждые 100 записей
		if count%100 == 0 {
			log.Printf("📥 Fetched %d records...", count)
		}
	}

	// Проверяем ошибки после итерации по строкам
	if err = rows.Err(); err != nil {
		log.Printf("❌ Error iterating rows: %v", err)
		returnJSONError(w, fmt.Sprintf("Error iterating rows: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("📥 Successfully fetched %d records from Firebird", count)

	// Проверяем, что есть данные для записи
	if len(staffCards) == 0 {
		log.Println("⚠️ No data found in Firebird")
		returnJSONError(w, "No data found in Firebird", http.StatusInternalServerError)
		return
	}

	// Подключаемся к PostgreSQL
	pgDB, err := connectPostgres()
	if err != nil {
		log.Printf("❌ PostgreSQL connection failed: %v", err)
		returnJSONError(w, fmt.Sprintf("PostgreSQL connection error: %v", err), http.StatusInternalServerError)
		return
	}
	defer pgDB.Close()

	// Инициализируем таблицу
	log.Println("🔄 Initializing PostgreSQL table...")
	err = initPostgresTable(pgDB)
	if err != nil {
		log.Printf("❌ Table initialization failed: %v", err)
		returnJSONError(w, fmt.Sprintf("Table initialization error: %v", err), http.StatusInternalServerError)
		return
	}

	// Записываем данные в PostgreSQL
	log.Println("📤 Writing data to PostgreSQL...")
	tx, err := pgDB.Begin()
	if err != nil {
		log.Printf("❌ Transaction start failed: %v", err)
		returnJSONError(w, fmt.Sprintf("Transaction error: %v", err), http.StatusInternalServerError)
		return
	}

	// Гарантируем откат транзакции в случае ошибки
	defer func() {
		if err != nil {
			tx.Rollback()
			log.Println("🔙 Transaction rolled back due to error")
		}
	}()

	// Очищаем таблицу перед записью новых данных
	log.Println("🧹 Clearing existing data...")
	_, err = tx.Exec("DELETE FROM staff_cards")
	if err != nil {
		log.Printf("❌ Error clearing table: %v", err)
		returnJSONError(w, fmt.Sprintf("Error clearing table: %v", err), http.StatusInternalServerError)
		return
	}

	// Обновляем время updated_at для всех записей
	updateTime := time.Now().Format("2006-01-02 15:04:05")

	stmt, err := tx.Prepare(`
		INSERT INTO staff_cards 
		(id_staff, identifier, last_name, first_name, middle_name, status, info, updated_at) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`)
	if err != nil {
		log.Printf("❌ Error preparing statement: %v", err)
		returnJSONError(w, fmt.Sprintf("Error preparing statement: %v", err), http.StatusInternalServerError)
		return
	}
	defer stmt.Close()

	// Вставляем данные
	insertCount := 0
	for _, sc := range staffCards {
		_, err := stmt.Exec(
			sc.IDStaff,
			sc.Identifier,
			sc.LastName,
			sc.FirstName,
			sc.MiddleName,
			sc.Status,
			sc.Info,
			updateTime,
		)
		if err != nil {
			log.Printf("❌ Error inserting data (ID_STAFF: %d, IDENTIFIER: %s): %v", sc.IDStaff, sc.Identifier, err)
			returnJSONError(w, fmt.Sprintf("Error inserting data: %v", err), http.StatusInternalServerError)
			return
		}
		insertCount++

		// Логируем прогресс каждые 100 записей
		if insertCount%100 == 0 {
			log.Printf("📤 Inserted %d records...", insertCount)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("❌ Error committing transaction: %v", err)
		returnJSONError(w, fmt.Sprintf("Error committing transaction: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("✅ Data update completed: %d records transferred at %s", len(staffCards), updateTime)
	returnJSONSuccess(w, map[string]interface{}{
		"records_updated": len(staffCards),
		"last_update":     updateTime,
	}, fmt.Sprintf("Updated %d records", len(staffCards)))
}

// searchAPIHandler обрабатывает API запросы для поиска по номеру карты
func searchAPIHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		returnJSONError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Получаем параметр card из query string
	cardNumber := r.URL.Query().Get("card")
	if cardNumber == "" {
		returnJSONError(w, "Missing 'card' parameter", http.StatusBadRequest)
		return
	}

	// Подключаемся к PostgreSQL
	pgDB, err := connectPostgres()
	if err != nil {
		log.Printf("❌ PostgreSQL connection failed: %v", err)
		returnJSONError(w, fmt.Sprintf("PostgreSQL connection error: %v", err), http.StatusInternalServerError)
		return
	}
	defer pgDB.Close()

	// Выполняем поиск по номеру карты
	query := `
		SELECT id_staff, identifier, last_name, first_name, middle_name, status, info
		FROM staff_cards
		WHERE identifier = $1
	`
	rows, err := pgDB.Query(query, cardNumber)
	if err != nil {
		log.Printf("❌ Search query failed: %v", err)
		returnJSONError(w, fmt.Sprintf("Search error: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []StaffCard
	for rows.Next() {
		var sc StaffCard
		var lastName, firstName, middleName, status, info sql.NullString

		err := rows.Scan(&sc.IDStaff, &sc.Identifier, &lastName, &firstName, &middleName, &status, &info)
		if err != nil {
			log.Printf("❌ Error scanning row: %v", err)
			returnJSONError(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
			return
		}

		if lastName.Valid {
			sc.LastName = &lastName.String
		}
		if firstName.Valid {
			sc.FirstName = &firstName.String
		}
		if middleName.Valid {
			sc.MiddleName = &middleName.String
		}
		if status.Valid {
			sc.Status = &status.String
		}
		if info.Valid {
			sc.Info = &info.String
		}

		results = append(results, sc)
	}

	if len(results) == 0 {
		returnJSONError(w, "Card not found", http.StatusNotFound)
		return
	}

	// Возвращаем первый найденный результат
	returnJSONSuccess(w, results[0], "Card found")
}

// searchHandler обрабатывает веб-запросы для поиска (HTML интерфейс)
func searchHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		returnJSONError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	searchTerm := r.URL.Query().Get("search")
	if searchTerm == "" {
		tmpl.Execute(w, nil)
		return
	}

	// Подключаемся к PostgreSQL
	pgDB, err := connectPostgres()
	if err != nil {
		http.Error(w, fmt.Sprintf("PostgreSQL connection error: %v", err), http.StatusInternalServerError)
		return
	}
	defer pgDB.Close()

	// Выполняем поиск
	query := `
		SELECT id_staff, identifier, last_name, first_name, middle_name, status, info
		FROM staff_cards
		WHERE last_name ILIKE $1 OR first_name ILIKE $1 OR middle_name ILIKE $1 OR identifier ILIKE $1
	`
	rows, err := pgDB.Query(query, "%"+searchTerm+"%")
	if err != nil {
		http.Error(w, fmt.Sprintf("Search error: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var results []StaffCard
	for rows.Next() {
		var sc StaffCard
		var lastName, firstName, middleName, status, info sql.NullString

		err := rows.Scan(&sc.IDStaff, &sc.Identifier, &lastName, &firstName, &middleName, &status, &info)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
			return
		}

		if lastName.Valid {
			sc.LastName = &lastName.String
		}
		if firstName.Valid {
			sc.FirstName = &firstName.String
		}
		if middleName.Valid {
			sc.MiddleName = &middleName.String
		}
		if status.Valid {
			sc.Status = &status.String
		}
		if info.Valid {
			sc.Info = &info.String
		}

		results = append(results, sc)
	}

	data := struct {
		SearchTerm string
		Results    []StaffCard
	}{
		SearchTerm: searchTerm,
		Results:    results,
	}

	tmpl.Execute(w, data)
}

// statsHandler возвращает статистику по данным
func statsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		returnJSONError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Подключаемся к PostgreSQL
	pgDB, err := connectPostgres()
	if err != nil {
		log.Printf("❌ PostgreSQL connection failed: %v", err)
		returnJSONError(w, fmt.Sprintf("PostgreSQL connection error: %v", err), http.StatusInternalServerError)
		return
	}
	defer pgDB.Close()

	// Получаем статистику
	var totalRecords int
	err = pgDB.QueryRow("SELECT COUNT(*) FROM staff_cards").Scan(&totalRecords)
	if err != nil {
		returnJSONError(w, fmt.Sprintf("Error getting stats: %v", err), http.StatusInternalServerError)
		return
	}

	// Получаем время последнего обновления
	var lastUpdate sql.NullString
	err = pgDB.QueryRow("SELECT MAX(updated_at) FROM staff_cards").Scan(&lastUpdate)
	if err != nil {
		returnJSONError(w, fmt.Sprintf("Error getting last update time: %v", err), http.StatusInternalServerError)
		return
	}

	lastUpdateStr := "Never updated"
	if lastUpdate.Valid {
		lastUpdateStr = lastUpdate.String
	}

	returnJSONSuccess(w, map[string]interface{}{
		"total_records": totalRecords,
		"last_update":   lastUpdateStr,
		"database":      config.PostgresDB,
		"description":   "last_update shows when data was last synchronized from Firebird",
	}, "Statistics retrieved")
}

func main() {
	// Проверка подключения к базам данных при запуске
	log.Println("🔍 Checking database connections...")

	// Проверка Firebird
	if err := checkFirebirdConnection(); err != nil {
		log.Printf("❌ Firebird connection check failed: %v", err)
	} else {
		log.Println("✅ Firebird connection check passed")
	}

	// Проверка PostgreSQL
	if err := checkPostgresConnection(); err != nil {
		log.Printf("❌ PostgreSQL connection check failed: %v", err)
		log.Fatal("Cannot start server without PostgreSQL connection")
	} else {
		log.Println("✅ PostgreSQL connection check passed")
	}

	// Инициализация таблицы PostgreSQL при старте
	pgDB, err := connectPostgres()
	if err != nil {
		log.Fatalf("❌ Failed to connect to PostgreSQL for table initialization: %v", err)
	}
	defer pgDB.Close()

	if err := initPostgresTable(pgDB); err != nil {
		log.Fatalf("❌ Failed to initialize PostgreSQL table: %v", err)
	}

	// Инициализация шаблонов
	var templateErr error
	tmpl, templateErr = template.ParseFiles("index.html")
	if templateErr != nil {
		log.Fatalf("❌ Error loading template: %v", templateErr)
	}

	// Настройка маршрутов
	http.HandleFunc("/", searchHandler)              // Веб-интерфейс поиска
	http.HandleFunc("/update", updateHandler)        // Обновление данных из Firebird
	http.HandleFunc("/api/search", searchAPIHandler) // API поиска по номеру карты
	http.HandleFunc("/api/stats", statsHandler)      // API статистики

	// Запуск сервера
	port := getEnv("PORT", "8080")
	log.Printf("🚀 Server starting on port %s", port)
	log.Printf("📊 Available endpoints:")
	log.Printf("   GET  /                 - Web interface for search")
	log.Printf("   POST /update           - Update data from Firebird")
	log.Printf("   GET  /api/search?card= - API search by card number")
	log.Printf("   GET  /api/stats        - API statistics")
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
