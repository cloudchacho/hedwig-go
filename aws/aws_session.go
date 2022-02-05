package aws

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

func createSession(
	region string, awsAccessKey string, awsSecretAccessKey string, awsSessionToken string) *session.Session {
	var creds *credentials.Credentials
	if awsAccessKey != "" && awsSecretAccessKey != "" {
		creds = credentials.NewStaticCredentialsFromCreds(
			credentials.Value{
				AccessKeyID:     awsAccessKey,
				SecretAccessKey: awsSecretAccessKey,
				SessionToken:    awsSessionToken,
			},
		)
	}
	return session.Must(session.NewSessionWithOptions(
		session.Options{
			Config: aws.Config{
				Credentials: creds,
				Region:      aws.String(region),
				DisableSSL:  aws.Bool(false),
			},
		}))
}

type sessionKey struct {
	awsRegion       string
	awsAccessKeyID  string
	awsSessionToken string
}

// SessionsCache is a cache that holds sessions
type SessionsCache struct {
	sessionMap sync.Map
}

// NewAWSSessionsCache creates a new session cache
func NewAWSSessionsCache() *SessionsCache {
	return &SessionsCache{
		sessionMap: sync.Map{},
	}
}

func (c *SessionsCache) getOrCreateSession(settings *Settings) *session.Session {
	key := sessionKey{awsRegion: settings.AWSRegion, awsAccessKeyID: settings.AWSAccessKey, awsSessionToken: settings.AWSSessionToken}
	s, ok := c.sessionMap.Load(key)
	if !ok {
		s = createSession(settings.AWSRegion, settings.AWSAccessKey, settings.AWSSecretKey, settings.AWSSessionToken)
		c.sessionMap.Store(key, s)
	}
	return s.(*session.Session)
}

// GetSession retrieves a session if it is cached, otherwise creates one
func (c *SessionsCache) GetSession(settings *Settings) *session.Session {
	return c.getOrCreateSession(settings)
}
