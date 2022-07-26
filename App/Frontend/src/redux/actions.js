// Declarar librería para los request HTTP
const axios = require("axios");

// Funcion general solo para hacer consultas Postgres
export function makeCall(query){
    return function (dispatch) {
        return axios.get('http://192.53.165.168:8000/query/', {
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


// Funcion para limpiar los datos que estan en pantalla
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


// Funcion para hacer una consulta rapida de la lista desplegable
export function Quick_query(quick){
    return function (dispatch) {
        return axios.get('http://192.53.165.168:8000/query/', {
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