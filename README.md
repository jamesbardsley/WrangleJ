# Overview

WrangleJ is an annotation library intended to make it simple to wrangle outputs
from APIs into Java objects. The intent is that you should be able to define a POJO,
annotate it with information about the data from the API and annotate each member with
information about where it can be found in the provided data. You are then provided
with a static method which takes the raw data as an argument and produces a stream of
your objects. This is intended to be easily usable with ORM libraries.

# Usage

When documenting usage we will define an imaginary API that defined two endpoints: 
`<ourapi>/students` and `<ourapi>/course`.


The output from the `<ourapi>/students` endpoint is a JSON list of objects which look
like this

```json
{
  "firstName": "James",
  "lastName": "McBob",
  "dob": "2005/02/12",
  "classId": 12,
  "pets": [
    {
      "name": "Fluffy",
      "type": "DOG"
    },
    {
      "name": "Whiskers",
      "type": "CAT"
    }
  ]
}
```

and the output from the `<ourapi>/course` endpoint is a JSON list of objects
which look like this:

```json
{
  "id": 12,
  "className": "Computer Science"
}
```

We will use WrangleJ to annotate this object:

```java
class Student {
    private String firstName;
    private String lastName;
    private String fullName;
    private String dob;
    private String className;
    private String catName;
}
```

After we have provided the appropriate annotations our Student class will have
a static `wrangledStream()` method which can be supplied the output from the
`<ourapi>/students` and `<ourapi>/course` endpoints and will produce a stream
of Student objects.

## `@WrangleSource` annotations
Every class using WrangleJ must be given at least one `@WrangleSource` annotation. The
`@WrangleSource` annotation defines the inputs for our `wrangledStream()` method.

For our `Student` class we provide the following annotations:

```java
@WranglePrimary(name="student")
@WrangleSource(name="class", joinLeft="student.classId", joinRight="class.id", joinType=INNER)
class Student {}
```

You will see the following parameters are used:

* **name**: The name we assign to an input. This will be used in all our subsequent
            annotations to refer to this source.
* **joinLeft**: The 

TODO: Document these fully.

## `@WrangleField` annotations

We can annotate our student fields in the following way:

```java
class Student {
    @WrangleField(direct="student.firstName")
    private String firstName;
    @WrangleField(direct="student.lastName")
    private String lastName;
    @WrangleField(concat("student.firstName", "student.lastName"))
    private String fullName;
    private String dob;
    private String className;
    private String catName;
}
```

## The `wrangledStream()` method

Every input should be an instance of `Iterable<Map<String, Object>>`