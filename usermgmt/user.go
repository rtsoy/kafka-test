package usermgmt

import "go.mongodb.org/mongo-driver/bson/primitive"

const UserManagementTopic = "usermgmt"

type User struct {
	ID   primitive.ObjectID `bson:"_id,omitempty"`
	Name string
	Age  int
}
