package de.tum.i13.server.kv;

public class KVMessageProcessor implements KVMessage{
//Sarra : i think there should be somehow instances here
    @Override
    public String getKey() {
        //key is the instance

        /*if ("key is in our cache")
        return
        * */

        return null;
    }

    @Override
    public String getValue() {
        //value is the instance

        /*if value==null
        return enum GET_ERROR
        else return enum GET_SUCCESS
        * */

        return null;
    }

    @Override
    public StatusType getStatus() {
        if (getKey()==null || getValue()==null)
            return StatusType.GET_ERROR;
        else return StatusType.GET_SUCCESS;
    }
}
