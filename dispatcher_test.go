package jobs

// func Test_Map_All(t *testing.T) {
// 	m := Dispatcher{"default": []string{"*"}}
// 	assert.Equal(t, "default", m.find("any"))
// }
//
// func Test_Map_Miss(t *testing.T) {
// 	m := Dispatcher{
// 		"default": []string{"some.*"},
// 	}
// 	assert.Equal(t, "", m.find("any"))
// }
//
// func Test_Map_Best(t *testing.T) {
// 	m := Dispatcher{
// 		"default": []string{"some.*"},
// 		"other":   []string{"some.other.*"},
// 	}
//
// 	assert.Equal(t, "default", m.find("some"))
// 	assert.Equal(t, "default", m.find("some.any"))
// 	assert.Equal(t, "other", m.find("some.other"))
// }
