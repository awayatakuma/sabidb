use super::query_data::QueryData;

#[derive(Debug, Clone)]
pub struct CreateViewData {
    viewname: String,
    qrydata: QueryData,
}

impl CreateViewData {
    pub fn new(viewname: String, qrydata: QueryData) -> Self {
        CreateViewData {
            viewname: viewname,
            qrydata: qrydata,
        }
    }

    pub fn view_name(&self) -> String {
        self.viewname.clone()
    }

    pub fn view_def(&self) -> String {
        self.qrydata.to_string()
    }
}
