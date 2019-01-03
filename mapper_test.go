package jobs

import (
	"github.com/magiconair/properties/assert"
	"testing"
)

func Test_Map_All(t *testing.T) {
	m := Mapper{"*": "default"}
	assert.Equal(t, "default", m.find("any"))
}

func Test_Map_Miss(t *testing.T) {
	m := Mapper{"some.*": "default"}
	assert.Equal(t, "", m.find("any"))
}

func Test_Map_Best(t *testing.T) {
	m := Mapper{
		"some.*":       "default",
		"some.other.*": "other",
	}

	assert.Equal(t, "default", m.find("some"))
	assert.Equal(t, "default", m.find("some.any"))
	assert.Equal(t, "other", m.find("some.other"))
}
