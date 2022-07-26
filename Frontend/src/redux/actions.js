const axios = require("axios");

export function makeCall(query){
    return function (dispatch) {
        return axios.get('http://45.56.77.151:8080/query/', {
            params: {
                consulta: query
            }
        })
        .then( res => res.data )
        .then( data => (dispatch({type: "GET_QUERY", payload: {
            data,
            consulta: query,
            query: ''
        } })))
    }
}

export function Clear(){
    return {
        type: 'CLEAR_STORE',
        payload: {
            query: '',
            datos: [{'Columna': "Fila"}],
            consulta: ''
        }
    }
}

export function Quick_query(quick){
    return function (dispatch) {
        return axios.get('http://45.56.77.151:8080/query/', {
            params: {
                consulta: quick
            }
        })
        .then( res => res.data )
        .then( data => (dispatch({type: "GET_QUERY", payload: {
            data,
            consulta: quick,
            query: ''
        } })))
    }
}