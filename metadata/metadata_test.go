package metadata

import (
	"fmt"
	"github.com/patrickmn/go-cache"
	"github.com/sirupsen/logrus"
	"math"
	"testing"
)

func TestName(t *testing.T) {
	ca := cache.New(0,0)
	for i := 0; i < 10; i++ {

		a := i+1
		ca.Set( fmt.Sprintf("%v",a), a, cache.NoExpiration)  // save 1

		result , ok := ca.Get(fmt.Sprintf("%v",i))  // get  0
		logrus.Info(result,ok)
	}
}

func TestInitMetadata(t *testing.T) {
	ca := []map[string]interface{}{
		{"id" : 1},
		{"id" : 2},
		{"id" : 3},
		{"id" : 4},
		{"id" : 5},
		{"id" : 6},
		{"id" : 7},
		{"id" : 8},
		{"id" : 9},
	}

	logrus.Println(ca[1])
	page := math.Ceil(float64(5)/float64(2))
	logrus.Println(page)


}