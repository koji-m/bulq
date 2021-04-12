use std::collections::HashMap;
use pyo3::prelude::*;
use pyo3::types::PyDict;

#[pymodule]
fn {{cookiecutter.plugin_module}}_inner(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DoFnMark>()?;
    m.add_class::<ExampleFn>()?;

    Ok(())
}

#[pyclass(subclass)]
pub struct DoFnMark {}

#[pymethods]
impl DoFnMark {
    #[new]
    fn new() -> Self {
        DoFnMark {}
    }
}

#[pyclass(extends=DoFnMark)]
pub struct ExampleFn {
    target: String
}

#[pymethods]
impl ExampleFn {
    #[new]
    fn new(target: String) -> (Self, DoFnMark) {
        (ExampleFn { target: target }, DoFnMark::new())
    }

    fn process(&self, record: &PyDict) ->PyResult<Vec<HashMap<String, String>>> {
        let mut rec = HashMap::<String, String>::new();

        let text: String = record.get_item(&self.target).unwrap().extract().unwrap();
        rec.insert(self.target.clone(), text.to_lowercase());

        let mut output = Vec::new();
        output.push(rec);
        Ok(output)
    }
}
