# Movement, a parallel dataflow system

Movement is a framework to build dataflow applications.  
It moves data items represented as an [Emitable](core/src/main/java/com/aerospike/movement/emitter/core/Emitable.java) from an Emitter to an Output via an Encoder.

An Emitable is a recursive information packet, defined by having a `Stream<Emitable> emit(Output)` function.  

This recursive datatype allows data elements to be decomposed as they move through the pipeline, creating a 1 -> N mapping.  

```text

Emitable --> Emitable
          \           / Emitable                          =>   Encoder   => Output
             Emitable 
                      \ Emitable

```

A file read into the input side may be converted into a Stream of Emitable lines. representing the decomposition of the file.  
These lines may be encoded to a database record, or some other format before being written to the output

Movement pipelines are composable. Encoders know how to represent data transformations, from a source format to a format understandable by the Output.    
The output can be a File or Directory, a Database system, or any system capable of receiving writes.

### Pipelined 
Every Movement Task is executed as 1 or more parallel pipelines. 

A Pipeline is an (Emitter)(Encoder)(Output) triple.  

The Driver controls the assignment of chunks of the input stream, and assigns them to the pipelines 

```
         /  (Pipeline: Emitter -> Encoder -> Output)
(Driver) -  (Pipeline: Emitter -> Encoder -> Output)
         \  (Pipeline: Emitter -> Encoder -> Output) 
```

### Modules
Support for file based operations is included, as well as an integration with [Apache TinkerPop](https://tinkerpop.apache.org/)


## License

This project is provided under the Apache2 software  [license](LICENSE).

## No Warranty
Movement is provided as-is and without warranty.
