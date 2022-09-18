package jwtutil

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go/v4"
)

// SignedString returns the complete, signed token
func SignedString(username, secret string, expiration time.Duration) (string, error) {
	token := jwt.New(jwt.GetSigningMethod("HS512"))
	token.Claims.(jwt.MapClaims)["username"] = username
	token.Claims.(jwt.MapClaims)["exp"] = time.Now().Add(expiration).Unix()
	return token.SignedString([]byte(secret))
}

// ParseToken parses a JWT token.
func ParseToken(tokenString, secret string) (jwt.MapClaims, error) {
	keyLookupFn := func(token *jwt.Token) (interface{}, error) {
		// Check for expected signing method.
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secret), nil
	}

	// Parse and validate the token.
	token, err := jwt.Parse(tokenString, keyLookupFn)
	if err != nil {
		return nil, err
	} else if !token.Valid {
		return nil, errors.New("invalid token")
	}

	// Make sure an expiration was set on the token.
	claims := token.Claims.(jwt.MapClaims)
	if exp, ok := claims["exp"].(float64); !ok || exp <= 0.0 {
		return nil, errors.New("token expiration required")
	}

	return claims, nil
}

// VerifyToken verifies that a JWT token is valid.
func VerifyToken(token *jwt.Token, parts []string, secret string) error {
	keyLookupFn := func(token *jwt.Token) (interface{}, error) {
		// Check for expected signing method.
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secret), nil
	}

	// Lookup key
	key, err := keyLookupFn(token)
	if err != nil {
		// keyFunc returned an error
		return wrapError(&jwt.UnverfiableTokenError{Message: "Keyfunc returned an error"}, err)
	}

	var vErr error

	// Perform validation
	token.Signature = parts[2]
	if err := token.Method.Verify(strings.Join(parts[0:2], "."), token.Signature, key); err != nil {
		vErr = wrapError(&jwt.InvalidSignatureError{}, err)
	}

	// Validate Claims
	if vErr == nil {
		if err := token.Claims.Valid(nil); err != nil {
			vErr = wrapError(err, vErr)
		}
	}

	if vErr == nil {
		token.Valid = true
	}
	if !token.Valid {
		return errors.New("invalid token")
	}

	// Make sure an expiration was set on the token.
	claims := token.Claims.(jwt.MapClaims)
	if exp, ok := claims["exp"].(float64); !ok || exp <= 0.0 {
		return errors.New("token expiration required")
	}

	return nil
}

// Embeds b within a, if a is a valid wrapper. returns a
// If a is not a valid wrapper, b is dropped
// If one of the errors is nil, the other is returned
func wrapError(a, b error) error {
	if b == nil {
		return a
	}
	if a == nil {
		return b
	}

	type iErrorWrapper interface {
		Wrap(error)
	}
	if w, ok := a.(iErrorWrapper); ok {
		w.Wrap(b)
	}
	return a
}
