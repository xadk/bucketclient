package bucketclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Session expiry in minutes, default value: 60 minutes,
// will deduct some minutes to save latency time
var (
	SESSION_EXPIRY = time.Minute*60 - time.Second*600
)

// Standard Metadata dictionary object
type Metadata map[string]interface{}

type APIV1Response struct {
	Version int         `json:"version"`
	Success bool        `json:"success"`
	Msg     string      `json:"msg"`
	Code    int         `json:"code"`
	Err     string      `json:"err"`
	Data    interface{} `json:"data"`
}

/**
 * User
 */

type User struct {
	UserID      int       `json:"user_id"`
	Username    string    `json:"username"`
	Email       string    `json:"email"`
	FirstName   string    `json:"first_name"`
	LastName    string    `json:"last_name"`
	Gender      string    `json:"gender"`
	Avatar      string    `json:"avatar"`
	Phone       string    `json:"phone"`
	Address     string    `json:"address"`
	Country     string    `json:"country"`
	ZipCode     string    `json:"zip_code"`
	DateOfBirth time.Time `json:"date_of_birth"`
	Roles       int       `json:"roles"`
	Metadata    Metadata  `json:"metadata"`
	UpdatedAt   time.Time `json:"updated_at"`
	JoinedAt    time.Time `json:"joined_at"`
}

/**
 * Session
 */

type SessionCreds struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SessionData struct {
	Token    string    `json:"token"`
	IssuedAt time.Time `json:"issuedAt"`
	Subject  int       `json:"subject"`
	Expiry   time.Time `json:"expiry"`
	User     User      `json:"user"`
}

/**
 * Buckets
 */

type Buckets []Bucket

// Implement the sort.Interface for BucketObjects

func (b Buckets) Len() int {
	return len(b)
}

// Since we want to reverse sort by object_id, we'll swap the comparison in Less
func (b Buckets) Less(i, j int) bool {
	return b[j].BucketID < b[i].BucketID
}

func (b Buckets) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

type Bucket struct {
	BucketID  int       `json:"bucket_id"`
	Alias     string    `json:"alias"`
	OwnerID   int       `json:"owner_id"`
	IsPublic  bool      `json:"is_public"`
	Metadata  Metadata  `json:"metadata"`
	UpdatedAt time.Time `json:"updated_at"`
	CreatedAt time.Time `json:"created_at"`
}

/**
 * Objects
 */

type Object struct {
	ObjectID       int       `json:"object_id"`
	Alias          string    `json:"alias"`
	ParentID       int       `json:"parent_id"`
	IsPublic       bool      `json:"is_public"`
	ContentType    string    `json:"content_type"`
	SQLContentType string    `json:"sql_content_type"`
	ContentLength  int       `json:"content_length"`
	Metadata       Metadata  `json:"metadata"`
	UpdatedAt      time.Time `json:"updated_at"`
	CreatedAt      time.Time `json:"created_at"`
}

type Objects []Object

// Implement the sort.Interface for BucketObjects

func (b Objects) Len() int {
	return len(b)
}

// Since we want to reverse sort by object_id, we'll swap the comparison in Less
func (b Objects) Less(i, j int) bool {
	return b[j].ObjectID < b[i].ObjectID
}

func (b Objects) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

/**
 * Bucket DB
 */

type BucketDB struct {
	// Data mutex
	mu *sync.Mutex

	// host
	host string

	// statics
	username, password string

	// Variables to track the session
	session SessionData

	// Error tracebacks
	traceback []error
}

func (db *BucketDB) Errorf(format string, v ...any) error {
	err := fmt.Errorf(format, v...)
	db.traceback = append(db.traceback, err)
	return err
}

/**
 * [CR]: Session
 */

func (db *BucketDB) IsValidSession() bool {
	return time.Now().Before(db.session.Expiry)
}

func (db *BucketDB) UpdateSession() error {
	body, err := json.Marshal(SessionCreds{
		Username: db.username,
		Password: db.password,
	})

	if err != nil {
		return err
	}
	data, err := db.apiV1RequestGeneric(METHOD_POST, "/api/v1/session", nil, bytes.NewReader(body))
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &db.session)
}

/**
 * [RU]: User
 */

func (db *BucketDB) Me() (*User, error) {
	var me User
	data, err := db.apiV1Request(METHOD_GET,
		"/api/v1/users/@me",
		nil,
		nil)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &me)
	if err != nil {
		return nil, err
	}

	(*db).session.User = me
	return &db.session.User, nil
}

func (db *BucketDB) UpdateMe(newMe User) (*User, error) {
	buf, err := json.Marshal(newMe)
	if err != nil {
		return nil, err
	}

	data, err := db.apiV1Request(METHOD_PUT,
		"/api/v1/users/@me",
		nil,
		bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	var user User
	err = json.Unmarshal(data, &user)
	if err != nil {
		return nil, err
	}

	(*db).session.User = user
	return &db.session.User, nil
}

/**
 * [CRUD]: Bucket
 */

func (db *BucketDB) CreateBucket(bucket Bucket) (*Bucket, error) {
	buf, err := json.Marshal(bucket)
	if err != nil {
		return nil, err
	}

	buf, err = db.apiV1Request(METHOD_POST,
		"/api/v1/buckets",
		nil,
		bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	var newBucket Bucket
	return &newBucket, json.Unmarshal(buf, &newBucket)
}

func (db *BucketDB) GetMyBucket(query string) (*Bucket, error) {
	var bucket Bucket
	data, err := db.apiV1Request(METHOD_GET,
		fmt.Sprintf("/api/v1/buckets/%s/%s", db.username, query),
		nil,
		nil)
	if err != nil {
		return nil, err
	}
	return &bucket, json.Unmarshal(data, &bucket)
}

func (db *BucketDB) GetMyBuckets(params url.Values) (*[]Bucket, error) {
	var buckets []Bucket
	var q string
	if qe := params.Encode(); qe != "" {
		q += "?" + qe
	}

	data, err := db.apiV1Request(METHOD_GET,
		fmt.Sprintf("/api/v1/buckets/%s"+q, db.username),
		nil,
		nil)
	if err != nil {
		return nil, err
	}

	return &buckets, json.Unmarshal(data, &buckets)
}

func (db *BucketDB) GetPublicBucket(userQuery, query string) (*Bucket, error) {
	var bucket Bucket
	data, err := db.apiV1Request(METHOD_GET,
		fmt.Sprintf("/api/v1/buckets/%s/%s", userQuery, query),
		nil,
		nil)
	if err != nil {
		return nil, err
	}
	return &bucket, json.Unmarshal(data, &bucket)
}

func (db *BucketDB) GetPublicBuckets(userQuery string, params url.Values) (*[]Bucket, error) {
	var buckets []Bucket
	var q string
	if qe := params.Encode(); qe != "" {
		q += "?" + qe
	}

	data, err := db.apiV1Request(METHOD_GET,
		fmt.Sprintf("/api/v1/buckets/%s"+q, userQuery),
		nil,
		nil)
	if err != nil {
		return nil, err
	}

	return &buckets, json.Unmarshal(data, &buckets)
}

func (db *BucketDB) UpdateBucket(query string, newBucket Bucket) (*Bucket, error) {
	buf, err := json.Marshal(newBucket)
	if err != nil {
		return nil, err
	}

	data, err := db.apiV1Request(
		METHOD_PUT,
		fmt.Sprintf("/api/v1/buckets/%s", query),
		nil,
		bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	var bucket Bucket
	return &bucket, json.Unmarshal(data, &bucket)
}

func (db *BucketDB) DeleteBucket(query string) error {
	_, err := db.apiV1Request(METHOD_DELETE,
		fmt.Sprintf("/api/v1/buckets/%s", query),
		nil,
		nil)
	return err
}

/**
 * [CRUD]: Objects
 */

func (db *BucketDB) CreateObject(bucketQuery string, obj Object) (*Object, error) {
	buf, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	buf, err = db.apiV1Request(METHOD_POST,
		fmt.Sprintf("/api/v1/objects/%s", bucketQuery),
		nil,
		bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	var newObj Object
	return &newObj, json.Unmarshal(buf, &newObj)
}

func (db *BucketDB) GetMyObject(bucketQuery, objQuery string) (*Object, error) {
	var obj Object
	data, err := db.apiV1Request(METHOD_GET,
		fmt.Sprintf("/api/v1/objects/%s/%s/%s", db.username, bucketQuery, objQuery),
		nil,
		nil)
	if err != nil {
		return nil, err
	}
	return &obj, json.Unmarshal(data, &obj)
}

func (db *BucketDB) GetMyObjects(bucketQuery string, params url.Values) (*[]Object, error) {
	var objects []Object
	var q string
	if qe := params.Encode(); qe != "" {
		q += "?" + qe
	}
	data, err := db.apiV1Request(METHOD_GET,
		fmt.Sprintf("/api/v1/objects/%s"+q, bucketQuery),
		nil,
		nil)
	if err != nil {
		return nil, err
	}

	return &objects, json.Unmarshal(data, &objects)
}

func (db *BucketDB) GetPublicObject(userQuery, bucketQuery, objQuery string) (*Object, error) {
	var obj Object
	data, err := db.apiV1Request(METHOD_GET,
		fmt.Sprintf("/api/v1/objects/%s/%s/%s", userQuery, bucketQuery, objQuery),
		nil,
		nil)
	if err != nil {
		return nil, err
	}
	return &obj, json.Unmarshal(data, &obj)
}

func (db *BucketDB) GetPublicObjects(userQuery, bucketQuery string, params url.Values) (*[]Object, error) {
	var objects []Object
	var q string
	if qe := params.Encode(); qe != "" {
		q += "?" + qe
	}

	data, err := db.apiV1Request(METHOD_GET,
		fmt.Sprintf("/api/v1/objects/%s/%s"+q, userQuery, bucketQuery),
		nil,
		nil)
	if err != nil {
		return nil, err
	}

	return &objects, json.Unmarshal(data, &objects)
}

func (db *BucketDB) UpdateObject(bucketQuery, objQuery string, newObj Object) (*Object, error) {
	buf, err := json.Marshal(newObj)
	if err != nil {
		return nil, err
	}

	data, err := db.apiV1Request(
		METHOD_PUT,
		fmt.Sprintf("/api/v1/objects/%s/%s", bucketQuery, objQuery),
		nil,
		bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	var obj Object
	return &obj, json.Unmarshal(data, &obj)
}

func (db *BucketDB) DeleteObject(bucketQuery, objQuery string) error {
	_, err := db.apiV1Request(METHOD_DELETE,
		fmt.Sprintf("/api/v1/objects/%s/%s", bucketQuery, objQuery),
		nil,
		nil)
	return err
}

// Streaming Object Contents
// Push / Pull

func (db *BucketDB) UploadObjectContent(bucketQuery, objQuery string, contentType string, body io.Reader) error {
	var headers url.Values = nil
	if contentType != "" {
		headers = url.Values{}
		headers.Set("Content-Type", contentType)
	}
	_, err := db.apiV1Request(METHOD_POST,
		fmt.Sprintf("/api/v1/objects/%s/%s/upload", bucketQuery, objQuery),
		headers,
		body)
	return err
}

func (db *BucketDB) FetchMyObjectContent(bucketQuery, objQuery string) (io.Reader, error) {
	// Session checks
	if !db.IsValidSession() {
		if err := db.UpdateSession(); err != nil {
			return nil, err
		}
	}
	// Fetching via public API with my username
	return db.FetchPublicObjectContent(db.username, bucketQuery, objQuery)
}

func (db *BucketDB) FetchPublicObjectContent(userQuery, bucketQuery, objQuery string) (io.Reader, error) {
	client := &http.Client{}

	// Request
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/objects/%s/%s/%s/content", db.host, userQuery, bucketQuery, objQuery), nil)
	if err != nil {
		return nil, err
	}

	// Headers
	req.Header.Set("Authorization", "Bearer "+db.session.Token)
	// Response
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

/**
 * BUCKETDB
 */

func NewBucketDB(host, username, password string) *BucketDB {
	var db *BucketDB = new(BucketDB)
	db.mu = &sync.Mutex{}
	db.host = host
	db.username = username
	db.password = password
	return db
}
