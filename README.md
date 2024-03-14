Movement is a parallel dataflow system.  It moves data items represented as an Emitable from an Emitter to an Output via an Encoder.  

The Emitable is a recursive information packet, defined by having a `Stream<Emitable> emit(output)` function.  
This recursive datatype allows data elements to be decomposed as they move through the pipeline, creating a 1 -> N mapping.  
A file read from the input side may in turn emit a Stream of lines, representing the decomposition of the file.  

Movement pipelines are composable. Encoders know how to represent data transformations, from a source format to a format understandable by the Output.    
The output can be a File or Directory, a Database system, or any system capable of receiving writes.

Every Movement Task is executed as 1 or more parallel pipelines. 


A Pipeline is an (Emitter)(Encoder)(Output) triple.  

The Driver controls the assignment of chunks of the input stream, and assigns them to the pipelines 

```
         /  (Pipeline: Emitter -> Encoder -> Output)
(Driver) -  (Pipeline: Emitter -> Encoder -> Output)
         \  (Pipeline: Emitter -> Encoder -> Output) 
```

