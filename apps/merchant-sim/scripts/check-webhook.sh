#!/usr/bin/env sh
# Verificación manual de T18 / R17 contra merchant-sim.
# Uso:  FAIL_RATE=0 docker compose up -d --build merchant-sim
#       sh apps/merchant-sim/scripts/check-webhook.sh
# Variables: URL (default http://localhost:4100/webhooks/pagolab), WEBHOOK_SECRET (default whsec_local),
#            N (webhooks para medir FAIL_RATE, default 0 = no medir; requiere FAIL_RATE>0 en el contenedor)

URL="${URL:-http://localhost:4100/webhooks/pagolab}"
SECRET="${WEBHOOK_SECRET:-whsec_local}"
N="${N:-0}"
fails=0

# hex( HMAC-SHA256( secreto, "<t>.<cuerpo>" ) )  — design § 7
sign() { printf '%s' "$1.$2" | openssl dgst -sha256 -hmac "$SECRET" | sed 's/^.*= //'; }

# post <cuerpo> [cabecera]  → imprime el código HTTP (000 = sin respuesta / timeout)
post() {
  if [ -n "$2" ]; then
    curl -s -o /dev/null -w '%{http_code}' --max-time 3 -X POST "$URL" \
      -H 'content-type: application/json' -H "pagolab-signature: $2" --data-binary "$1"
  else
    curl -s -o /dev/null -w '%{http_code}' --max-time 3 -X POST "$URL" \
      -H 'content-type: application/json' --data-binary "$1"
  fi
}

check() {  # check <descripción> <esperado> <obtenido>
  if [ "$2" = "$3" ]; then printf '  OK    %-42s %s\n' "$1" "$3"
  else printf '  FALLA %-42s esperado %s, obtenido %s\n' "$1" "$2" "$3"; fails=$((fails + 1)); fi
}

now=$(date +%s)
id="evt_check_$now"
body="{\"eventId\":\"$id\",\"type\":\"payment.approved\",\"data\":{\"id\":\"pay_1\"}}"
sig=$(sign "$now" "$body")

echo "merchant-sim en $URL"
check "firma válida"                        200 "$(post "$body" "t=$now,v1=$sig")"
check "mismo eventId otra vez (duplicado)"  200 "$(post "$body" "t=$now,v1=$sig")"

tampered=$(printf '%s' "$body" | sed 's/pay_1/pay_2/')
check "cuerpo alterado"                     401 "$(post "$tampered" "t=$now,v1=$sig")"
check "t alterado sin volver a firmar"      401 "$(post "$body" "t=$((now - 1)),v1=$sig")"

old=$((now - 600))
check "t de hace 10 minutos"                400 "$(post "$body" "t=$old,v1=$(sign "$old" "$body")")"
check "sin cabecera de firma"               400 "$(post "$body")"
check "cabecera sin v1"                     400 "$(post "$body" "t=$now")"
check "cuerpo no JSON con firma válida"     400 "$(post 'hola' "t=$now,v1=$(sign "$now" 'hola')")"

shape='{"eventId":"evt_sin_data"}'
check "JSON sin data.id con firma válida"   400 "$(post "$shape" "t=$now,v1=$(sign "$now" "$shape")")"

if [ "$N" -gt 0 ]; then
  errors=0; i=1
  while [ "$i" -le "$N" ]; do
    b="{\"eventId\":\"evt_rate_${now}_$i\",\"type\":\"payment.approved\",\"data\":{\"id\":\"pay_$i\"}}"
    [ "$(post "$b" "t=$now,v1=$(sign "$now" "$b")")" = 500 ] && errors=$((errors + 1))
    i=$((i + 1))
  done
  echo "  FAIL_RATE: $errors/$N webhooks válidos respondieron 500 ($((errors * 100 / N)) %)"
fi

[ "$fails" -eq 0 ] && echo "Todo en verde." || echo "$fails caso(s) fallaron."
exit "$fails"
